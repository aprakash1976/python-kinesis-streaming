import asyncio
import argparse
import sys
import logging
import time
import calendar
import os
import daemon
import collections
import boto
import msgpack
import concurrent.futures
import signal
import threading
import io
import re
import random
import hashlib
import daemon
import platform
from daemon.pidfile import TimeoutPIDLockFile
import affine.config
from affine.uuids import generate_id
import Queue
from streaming.util import IntervalDispatcher
from affine.set_librato import SetLibrato
from affine.retries import retry_operation

_DEFAULT_UNIX_PATH = '\0/einterposer'
_DEFAULT_DATA_DIR = affine.config.get("einterposer.datadir")
_DEFAULT_CHECKPOINT_FILE = affine.config.get("einterposer.checkpoint_file")
_DEFAULT_PORT = 9432

def ftwrap(targetobj, method, *args):
    return getattr(targetobj, method)(*args)



def snap_next_min_window(now, time_window):
    g = list(time.gmtime(now))
    g[4] = (g[4]/time_window + 1)*time_window
    g[5] = 0
    return calendar.timegm(g)
    
def HUP_handler(eiposer):
    return eiposer.handle_hup()

def TERM_handler(eiposer):
    try:
        eiposer.cleanup()
    finally:
        sys.exit(0)

def event_flush_to_file(sequencer, records, dorotate):
    sequencer.do_flush(records, dorotate)

def do_checkpoint(eiposer):
    eiposer.checkpoint()

def checkpoint_save(eiposer, bundler, curseq):
    eiposer.checkpoint_save(bundler, curseq)

def checkpoint_recover(eiposer):
    eiposer.checkpoint_recover()

def bundler_emit(bundler, partid, bundle):
    bundler.emit(partid, bundle)

def directory_read(parent_dir, tstart):
    elts = [ os.path.join(parent_dir, df) for df in os.listdir(parent_dir) ]
    ldir = sorted(filter(os.path.isdir, elts))
    lfile = sorted(filter(os.path.isfile, elts))

    for d in ldir:
        for df in directory_read(d, tstart):
            yield df

    fregex = re.compile(r'.+\/(\d{2})_(\d{2})\/(\d{2})\/eip_(\d{2})_(\d{2})\.+')
    for f in lfile:
        mo = fregex.match(f)
        if (mo):
            tm = map(int, mo.groups())
            fts = calendar.timegm([tm[0]+2000] + tm[1:] + [0]*4)
            if (fts >= tstart):
                yield f


def seek_by_time_and_seq(topdir, tseqstart):
    for fpath in directory_read(topdir, tseqstart.ts):
        dunpacker = msgpack.Unpacker()
        with io.FileIO(fpath, mode="r") as dfd:
            while (True):
                chunk = dfd.read(2048)
                if (len(chunk) == 0): break
                dunpacker.feed(chunk)
                for rec in dunpacker:
                    rec['seq'] = TSSeq(rec['seq'])
                    if (rec['seq'] > tseqstart):
                        yield rec



class TSSeq(tuple):
    def __init__(self, *args):
        super(TSSeq, self).__init__(*args)

    @property
    def ts(self): return self[0]

    @property
    def seqnum(self): return self[1]

    def __str__(self):
        return "%d.%06x" % (self.ts, self.seqnum)
    
    def __format__(self, fstr):
        return "%d.%06x" % (self.ts, self.seqnum)

    def __cmp__(self, other):
        if (self.ts < other.ts): return -1
        if (self.ts > other.ts): return 1
        if (self.seqnum < other.seqnum): return -1
        if (self.seqnum > other.seqnum): return 1
        return 0

    @classmethod
    def fromstring(cls, fmtstr):
        (ts, seqnum) = fmtstr.split('.')
        return cls((int(ts), int(seqnum,16)))

    def __getnewargs__(self):
        return ((self.ts, self.seqnum),)
        
            


class Bundler(object):
    _MAX_WORKERS = 50
    _DEFAULT_BUNDLE_MAXAGE = 90
    def __init__(self, stream_name, *args, **kwargs):
        self._stream_name = stream_name
        self._bundlesize = kwargs.get('bundlesize', 30)
        self._loop       = kwargs.get('loop', None)
        self._maxworkers = kwargs.get('maxworkers', self._MAX_WORKERS)
        self._thrpool    = kwargs.get('thrpool', concurrent.futures.ThreadPoolExecutor(self._maxworkers))
        self._log        = kwargs.get('logger', logging.getLogger(__name__))
        self._partition_key = kwargs.get('partition_key', 'partition_key')
        self._bundle_maxage = kwargs.get('lifetime', self._DEFAULT_BUNDLE_MAXAGE)
        self._log.setLevel(logging.INFO)
        self._kin        = boto.connect_kinesis()
        self._filtered   = 0
        self._ooo        = 0
        self._lasttsseq  = TSSeq((0,0))
        self._pfutures   = []
        self._emitq = Queue.Queue()
        self._cntr_lock  = threading.Lock()
        self._part_lock  = threading.Lock()
        self._ft_lock = threading.Lock()
        self._tlock   = threading.Lock()
        self._kput_count = 0
        self._put_metrics = collections.deque((),50)
        self._last_print = time.time()
        self._putstart = {}
        self.configure_stream()

    def thractive(self):
        return len(self._pfutures)

    @property
    def outoforder(self):
        return (self._ooo != 0)
        
    def configure_stream(self):
        siresp = self._kin.describe_stream(self._stream_name)
        strinfo = siresp['StreamDescription']
        self._log.debug('Stream info: {0}'.format(str(strinfo)))
        self._partcount = len(strinfo['Shards'])
        self._log.info('Shard count: {0}'.format(self._partcount))
        #
        #  Precompute keys that will map to the Kinesis shards
        # 
        self._shhi = [ int(sh['HashKeyRange']['EndingHashKey']) for sh in strinfo['Shards'] ]
        self._log.debug('Partition HI values: {0}'.format(self._shhi))
        self._pkey = {}
        for pidx in xrange(self._partcount):
            sh = strinfo['Shards'][pidx]
            (lo, hi) = (int(sh['HashKeyRange']['StartingHashKey']), int(sh['HashKeyRange']['EndingHashKey']))
            while (pidx not in self._pkey):
                key = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 8))
                ival = int(hashlib.md5(key).hexdigest(), 16)
                if (ival > lo) and (ival < hi):
                    self._pkey[pidx] = key
        self._log.debug('Partition Keys: {0}'.format(str(self._pkey)))
        self._partbun   = [[]] * self._partcount
        self._part_ts   = [None] * self._partcount

    def _emit(self, partid, bundle):
        # thread
        pmsg = msgpack.packb(tuple(bundle), use_bin_type=True)
        del bundle
        t0 = time.time()
        resp = None
        for retries in xrange(3):
            try:
                resp = self._kin.put_record(self._stream_name, pmsg, self._pkey[partid])
            except Exception as e:
                self._log.warn("kinesis put exception: {0}  retries:{1}".format(e,retries))
            else:
                break
        if (not resp):
            self._log.warn("failed kinesis put")
            return
        
        tnow = time.time()
        elap = tnow - t0
        with self._cntr_lock:
            self._kput_count += 1
            self._put_metrics.append((tnow, elap))
            if ((self._kput_count % 2000) == 0):
                self._log.info("kinesis put# {0:9d}   q-backlog: {1:5d}   thr-active: {2:2d}    elap:{3:7.3f} (avg:{4:7.3f})  rate:{5:7.3f}".format(
                        self._kput_count, self._emitq.qsize(), len(self._pfutures), elap, self.putrttavg, self.putrate))

    @property
    def putrttavg(self):
        if (len(self._put_metrics)):
            return sum((x[1] for x in self._put_metrics))/len(self._put_metrics)
        return 0.0

    @property
    def putrate(self):
        tnow = time.time()
        if (len(self._put_metrics)>10):
            deltat = tnow - self._put_metrics[0][0]
            if (deltat > 0.1):
                return float(len(self._put_metrics))/deltat
        return 0.0

    def send_chain(self):
        # thread
        myident = threading.currentThread().ident
        while (True):
            try:
                (partid, bundle) = self._emitq.get(False)
            except Queue.Empty:
                break
            else:
                with self._tlock:
                    self._putstart[myident] = time.time()
                self._emit(partid, bundle)
        with self._tlock:
            del self._putstart[myident]
        return True

    def backlog(self):
        return self._emitq.qsize()

    def reap_futures(self):
        with self._ft_lock:
            ftv = self._pfutures
            self._pfutures = filter((lambda ft: not ft.done()), ftv)

    def partindex(self, pkey):
        keyval = int(hashlib.md5(pkey).hexdigest(), 16)
        for (idx, shhi) in enumerate(self._shhi):
            if (keyval <= shhi):
                return idx
        raise Exception('shard ranges incorrect')

    def puttimes(self):
        ptimes = []
        with self._tlock:
            ptimes = self._putstart.values()
        return ptimes
    
    def recordcount(self):
        return sum((len(bun) for bun in self._partbun))

    def flush(self):
        self._log.info('bundler flushing [{0}] records to Kinesis'.format(self.recordcount()))
        for partid in xrange(len(self._partbun)):
            self._partition_flush(partid)

    def _partition_flush(self, partid):
        self.reap_futures()
        with self._part_lock:
            bundle = self._partbun[partid]
            self._partbun[partid] = []
        self._emitq.put((partid, bundle))
        if (len(self._pfutures) < self._maxworkers):
            self._log.debug("Spawning kinesis emit thread")
            ft = self._thrpool.submit(ftwrap, self, 'send_chain')
            self._pfutures.append(ft)

    def flush_aged_buffers(self):
        tnow = time.time()
        flush_cnt = 0
        avgage = 0.0
        for partid in xrange(self._partcount):
            if (len(self._partbun[partid]) and (self._part_ts[partid])):
                age = tnow - self._part_ts[partid]
                if (age > self._bundle_maxage):
                    self._partition_flush(partid)
                    flush_cnt += 1
                    avgage += age
        if (flush_cnt):
            avgage = avgage / flush_cnt
            self._log.info("flushed aged bundles:{0}  avg age: {1:.0f}s".format(flush_cnt, avgage))

    def _getpartitionkey(self, record):
        if (self._partition_key in record):
            return record[self._partition_key]
        elif ('partition_key' in record):
            return record['partition_key']
        else:
            return generate_id()

    def add(self, seqrecord):
        # 
        #  reject records out-of-order, 
        #
        seq  = seqrecord['seq']
        if (seq <= self._lasttsseq):
            self._ooo += 1
            if (seq == self._lasttsseq):
                self._log.info("Add record matches last record added {0}, bundler is now up-to-date".format(seq))
                self._ooo = 0
            return

        self._lasttsseq = seq
        record = seqrecord["record"]
        partkey = self._getpartitionkey(record)
        partid = self.partindex(partkey)
        if (len(self._partbun[partid]) >= self._bundlesize):
            self._partition_flush(partid)

        with self._part_lock:
            if (len(self._partbun[partid]) == 0):
                self._part_ts[partid] = time.time()
            self._partbun[partid].append(record)

    def __len__(self):
        return reduce((lambda x,y: x+y), map(len, self._partbun), 0)
        

class MTFiler(object):
    def __init__(self, path, **kwargs):
        self._writer_lock = threading.Lock()
        self._queue_lock = threading.Lock()
        self._qofq = collections.deque()
        self._log = kwargs['logger']
        self._wrenabled = True
        self._filer = io.FileIO(path, mode="w")

    def write(self, qrecords, drain):
        if (not self._wrenabled): return False
        with self._queue_lock:
            self._qofq.append(qrecords)
            if (drain):
                self._wrenabled = False
                self._qofq.append(())  # EOF sentinel

        if self._writer_lock.acquire(False):
            try:
                self._log.debug("acquired writer lock")
                packer = msgpack.Packer()
                while (len(self._qofq) > 0):
                    q = None
                    with self._queue_lock:
                        if (len(self._qofq)>0):
                            q = self._qofq.popleft()
                    if (len(q) == 0):
                        self._filer.close()
                        return True
                    elif (q is not None):
                        self._log.debug("writing block records:%d" % len(q))
                        while (q):
                            self._filer.write(packer.pack(q.popleft()))
                        del q
                        time.sleep(0.1)
            finally:
                self._writer_lock.release()
            self._log.debug("write queue empty")
        return True

    def close(self):
        with self._writer_lock:
            self._filer.close()
            self._filer = None





class Sequencer(object):
    def __init__(self, *args, **kwargs):
        self._log  = kwargs.get('logger', logging.getLogger(__name__))
        self._checkpoint  = kwargs.get('checkpoint', _DEFAULT_CHECKPOINT_FILE)
        self._wrchunksize = kwargs.get('chunksize', 50000)
        self._filetwindow = kwargs.get('filewindow', 5)
        self._topdir      = kwargs.get('dir', _DEFAULT_DATA_DIR)
        self._thrpool     = kwargs.get('thrpool', concurrent.futures.ThreadPoolExecutor(max_workers=4))
        self._loop        = kwargs.get('loop', asyncio.get_event_loop())
        self._bundler     = kwargs.get('bundler')
        self._stream_name = kwargs['stream_name']
        self._tnow = int(time.time())
        self._iseq = 0
        self._recq = collections.deque()
        self._next_snap = snap_next_min_window(self._tnow, self._filetwindow)
        if (not os.path.exists(self._topdir)):
            os.makedirs(self._topdir)
        dpath = self.make_filepath(self._next_snap)
        self._log.info(" Datafile path: {0}".format(dpath))
        self._rotate_filer_lock = threading.Lock()
        self._filer = MTFiler(dpath, logger=self._log)
        self._filecount = 0
        self._disabled = False
        self._recovering  = False
        self._reccnt      = 0

    def __len__(self):
        return len(self._recq)

    def do_flush(self, records, dorotate):
        with self._rotate_filer_lock:
            filer = self._filer
            if (dorotate):
                self._filecount += 1
                dpath = self.make_filepath(self._next_snap)
                self._log.debug(" Datafile path: {0}".format(dpath))
                self._filer = MTFiler(dpath, logger=self._log)
        filer.write(records, dorotate)

    def drain(self):
        self._log.info('Detaching and flushing bundler')
        bundler = self._bundler
        self._bundler = None
        bundler.flush()
        self._log.info('Draining sequencer, flushing to file and closing, CurrentSeq: {0}'.format(self.current_seq()))
        self._disabled = True
        self.do_flush(self._recq, False)
        self._filer.close()

    def checkpoint_save(self, bundler, curseq):
        self._log.info('Checkpointing - saving state @ {0}'.format(curseq))
        with io.open(self._checkpoint, mode='w') as chkpt:
            chkpt.write(unicode("%d.%d\n" % curseq))
        self._log.info('Checkpoint complete')

    def checkpoint_recover(self):
        #
        #  Get last checkpoint tsseq
        #
        lastchkpt = None
        self._log.info('Checkpoint recover - using {0} file'.format(self._checkpoint))
        with io.open(self._checkpoint, mode='r') as chkpt:
            fline = chkpt.readline()
            if (fline):
                lastchkpt = TSSeq.fromstring(fline.encode('ascii').strip("\n"))
        if (not lastchkpt):
            self._log.error("Failed reloading checkpoint seq from {0}".format(self._checkpoint))
        #
        #  Iterate through past records, adding to bundler
        #
        if (self._stream_name is None):
            self._log.warn("Stream is undefined.. cannot recover checkpoint")
            return
        bundler = Bundler(self._stream_name, loop=self._loop)
        self._log.info("Recover - created new Bundler {0}".format(id(bundler)))
        self._log.info('Checkpoint recover - seeking to {0}'.format(lastchkpt))
        cnt = 0
        for rec in seek_by_time_and_seq(self._topdir, lastchkpt):
            if (cnt == 0):
                self._log.info("Recover - first read: {0}".format(rec['seq']))
            cnt += 1
            bundler.add(rec)
        curseq = self.current_seq()
        self._log.info("Recover - last read: {0}  current: {1}".format(rec['seq'], curseq))
        self._recovering = False
        self._bundler = bundler

    def checkpoint(self):
        if (self._stream_name is None):
            self._log.warn("Caught HUP --- No associated stream to checkpoint/recover - ignoring")
            return
        if (self._bundler is not None):
            #
            #  Attached state --> detach and save checkpoint (all atomic)
            #
            bundler = self._bundler
            self._bundler = None
            curseq = self.current_seq()
            # now threaded
            self._loop.run_in_executor(self._thrpool, checkpoint_save, self, bundler, curseq)
            bundler.flush()
        else:
            if (self._recovering):
                self._log.warn("recovery is already in progress... cannot start another recovery")
                return
            self._log.info('attempting recovery')
            self._recovering = True
            #
            #  Detached state --> recover and catchup
            #
            self._loop.run_in_executor(self._thrpool, checkpoint_recover, self)

    @property
    def seqnum(self):
        return self._iseq

    def current_seq(self):
        return TSSeq((self._tnow, self._iseq))
    
    def make_filepath(self, ts, suffix="mpk"):
        tm = time.gmtime(ts)
        p = os.path.join(self._topdir, "%02d_%02d" % ((tm.tm_year-2000), tm.tm_mon), "%02d" % tm.tm_mday)
        if (not os.path.exists(p)):
            os.makedirs(p)
        return os.path.join(p, "eip_%02d_%02d.%s" % (tm.tm_hour, tm.tm_min, suffix))

    def next_seq(self):
        do_rotate = False
        t = int(time.time())
        if (t > self._tnow):
            self._iseq = 0
            self._tnow = t
            if (self._tnow >= self._next_snap):
                self._next_snap = snap_next_min_window(self._tnow, self._filetwindow)
                self._log.debug("rotating file @ {0}   nextsnap: {0}".format(self._tnow, time.asctime(time.gmtime(self._next_snap))))
                do_rotate = True
        else:
            self._iseq += 1
        return (TSSeq((self._tnow, self._iseq)), do_rotate)

    def detach_flush(self, dorotate):
        records = self._recq
        self._recq = collections.deque()
        self._loop.run_in_executor(self._thrpool, event_flush_to_file, self, records, dorotate)
        return

    def append(self, record):
        #
        #  Add to sequencer
        #
        self._reccnt += 1
        if (self._disabled): return
        last_seqnum = self.current_seq()
        (tsseq, dorotate) = self.next_seq()
        if (len(self._recq) >= self._wrchunksize) or (dorotate):
            self._log.debug("detach_flush: last: {0}".format(last_seqnum))
            self.detach_flush(dorotate)
        nr = dict(seq=tsseq, record=record)
        self._recq.append(nr)
        #
        #  Add to bundler queue, Drain queue if bundler attached
        #
        self._bundler.add(nr)
        if ((self._reccnt % 1000) == 0):
            self._log.debug("RQ: {0}  Kbacklog: {1}".format(len(self._recq), self._bundler.backlog()))

    def backlog(self):
        return self._bundler.backlog()


class EInterposer(threading.Thread):
    _LIBRATO_TOPIC = "streaming.interposer."
    _LIBRATO_INTERVAL = 240
    _DEFAULT_LOGINTERVAL = 300
    def __init__(self, *args, **kwargs):
        super(EInterposer, self).__init__()
        if ('logger' not in kwargs):
            kwargs['logger'] = logging.getLogger(__name__)
        self._log        = kwargs['logger']
        self._loop       = kwargs.get('loop', asyncio.get_event_loop())
        self._thrpool    = kwargs.get('thrpool', concurrent.futures.ThreadPoolExecutor(max_workers=6))
        self._log.setLevel(kwargs.get('debug', False) and logging.DEBUG or logging.INFO)
        self._loginterval = kwargs.get('loginterval', self._DEFAULT_LOGINTERVAL)
        self._sweepinterval = 10.0
        self._clients       = {}
        self._backlog_limit = float(100)
        self._backlog_wait  = 0.010
        self._stream_name = kwargs['stream_name']
        self._bundler     = Bundler(self._stream_name, loop=self._loop, partition_key=kwargs.get('partition_key'))
        self._sequencer  = Sequencer(bundler=self._bundler, **kwargs)
        self._tpostmetrics = IntervalDispatcher(self._LIBRATO_INTERVAL, self, 'postmetrics', daemon=True)
        env = affine.config.get("env") or "development"
        prefix = ((env+'.'), '')[env == 'production']
        self._libr_client = SetLibrato(affine.config, prefix + self._LIBRATO_TOPIC)
        self._libr_source = platform.node()
        self._log.info("einterposer initialized")
        self.daemon = True
        self._started = False

    def _max_put_time(self):
        tnow = time.time()
        ptimes = [(tnow - t) for t in self._bundler.puttimes()] + [0.0]
        return max(ptimes)

    def postmetrics(self):
        metrics = {
            'kinbacklog': self._bundler.backlog(),
            'putrate': self._bundler.putrate,
            'putrtt_avg' : self._bundler.putrttavg,
            'maxputtime' : self._max_put_time()
            }
        tnow = int(time.time())
        gauge_opts = dict(measure_time=tnow, source=self._libr_source)
        librmetrics = {}
        for gauge,val in metrics.items():
            librmetrics[gauge] = dict(gauge=val)
            librmetrics[gauge].update(gauge_opts)
        retry_operation(self._libr_client.bulk_gauge, librmetrics, sleep_time=2)

    def estart(self):
        if (not self._started):
            self.start()
            self._tpostmetrics.dispatch()
            self._started = True
            self._log.info("einterposer started")

    def run(self):
        tlog = time.time()
        while (True):
            time.sleep(self._sweepinterval)
            self._bundler.flush_aged_buffers()
            tnow = time.time()
            if ((tnow - tlog)>=self._loginterval):
                self._bundler.reap_futures()
                tlog = time.time()
                useqsum = sum([c[3] for c in self._clients.values()]) % 1000000
                self._log.info("Clients:{0}  USeqSum:{1}  BuffRecs:{2}  Qlen:{3}  Thr:{4}  MaxPutms:{5}".format(
                        len(self._clients), useqsum, 
                        self._bundler.recordcount(),
                        self._bundler.backlog(),
                        self._bundler.thractive(),
                        int(self._max_put_time()*1000)))

    def handle_hup(self):
        self._loop.run_in_executor(self._thrpool, do_checkpoint, self)

    def checkpoint(self):
        self._log.warn('Caught HUP seq @ {0}'.format(self._sequencer.current_seq()))
        self._sequencer.checkpoint()

    def cleanup(self):
        self._log.info("shutting down: active_clients: {0}".format(len(self._clients)))
        for (creader,cwriter) in self._clients.values():
            cwriter.close()
        self._sequencer.drain()

    def on_connect(self, sreader, swriter):
        self._log.info("new connection")
        myid = (id(sreader),id(swriter))
        self._clients[myid] = [sreader, swriter, 0, 0]
        strunpacker = msgpack.Unpacker()
        kupid = None
        kuseq = None        
        while (True):
            chunk = yield sreader.read(2048)
            if (len(chunk) == 0): break
            strunpacker.feed(chunk)
            for blob in strunpacker:
                if (not isinstance(blob,tuple)) and (len(blob)!=3):
                    x = yield swriter.write(msgpack.packb((0, 0), use_bin_type=True))
                    self._log.warn("data stream corrupted, resetting connection")
                    break
                else:
                    (upid, useq, recs) = blob
                    if (kuseq) and (useq != kuseq+1):
                        self._log.warn("OOS [{0}]  exp {1}  got {2})".format(upid, (kuseq+1), useq))
                    kuseq = useq
                    self._clients[myid][2] = upid
                    self._clients[myid][3] = useq
                    resp = msgpack.packb((upid, useq), use_bin_type=True)
                    x = yield swriter.write(resp)
                    for rec in recs:
                        self._sequencer.append(rec)
                   
        self._log.warn("client closed connection")
        del self._clients[myid]


def listener(**kwargs):
    opts = collections.defaultdict(bool, kwargs)
    asyncio.tasks._DEBUG = True
    loop = asyncio.get_event_loop()
    eiposer = EInterposer(loop=loop, **opts)
    def einterposer_connect_cb(sreader, swriter):
        yield eiposer.on_connect(sreader, swriter)
    loop.add_signal_handler(signal.SIGHUP, HUP_handler, eiposer)
    loop.add_signal_handler(signal.SIGTERM, TERM_handler, eiposer)
    eiposer.estart()
        
    if (opts['internet']):
        pipeline_svc = asyncio.start_server(einterposer_connect_cb, port=opts.get('port', _DEFAULT_PORT), loop=loop)
    else:
        pipeline_svc = asyncio.start_unix_server(einterposer_connect_cb, path=opts.get('path', _DEFAULT_UNIX_PATH), loop=loop)
    loop.run_until_complete(pipeline_svc)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        eiposer.cleanup()


def main_program(opts):
    if (not opts['interactive']):
        from affine.log import configure_logging, affine_handler
        configure_logging("log/einterposer.log")
        alog = logging.getLogger('asyncio')
        alog.setLevel(logging.WARN)
        alog.addHandler(affine_handler())
    else:
        logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(opts['debug'] and logging.DEBUG or logging.INFO)
    logger.info("Starting einterposer with options: {0}".format(','.join((k+':'+str(v) for (k,v) in opts.items()))))
    
    if (not opts['stream_name']):
        logger.info('running without Kinesis connection')
    #
    # (highly improbable) race condition
    #
    upath = opts.get('path', _DEFAULT_UNIX_PATH)
    if (not upath.startswith('\0') and (os.path.exists(upath))):
        if (opts['force']):
            os.remove(upath)
        else:
            logger.error("Cannot start einterposer: UNIX path [{0}] exists".format(upath))
            sys.exit(1)
    #
    #  kinesis sanity check
    #
    if (opts['stream_name']):
        try:
            kinconn = boto.connect_kinesis()
        except:
            logger.warn('Failed to connect to Kinesis -- no events will be emitted, only file-based logging')
            kinconn = None
    
        if (kinconn):
            siresp = kinconn.describe_stream(opts['stream_name'])
            if (not siresp) or ('StreamDescription' not in siresp):
                logger.error('failed to obtain kinesis stream information: {0}, aborting'.format(opts['stream_name']))
                sys.exit(1)
            kinconn.close()
    #
    listener(**opts)


if __name__ == "__main__":
    #
    #  cmd-line args
    #
    aparse = argparse.ArgumentParser()
    aparse.add_argument('-I', dest='interactive', action='store_true')
    aparse.add_argument('-V', dest='debug', action='store_true')
    aparse.add_argument('-i', dest='internet', action='store_true')
    aparse.add_argument('-p', dest='port', type=int)
    aparse.add_argument('-P', dest='path', type=str)
    aparse.add_argument('-k', dest='loginterval', type=int)
    aparse.add_argument('-K', dest='partition_key', type=str)
    aparse.add_argument('stream_name')
    popts = aparse.parse_args()
    opts = dict(((k,v) for (k,v) in vars(popts).items() if (v is not None)))

    if (not opts['interactive']):
        errpath = os.path.join(affine.config.log_dir(), "log", "einterposer.stderr")
        gerr = open(errpath, "w")
        dcontext = daemon.DaemonContext(working_directory='/tmp',
                                        pidfile=TimeoutPIDLockFile('/tmp/einterposer.pid'),
                                        stderr=gerr)
        with dcontext:
            main_program(opts)
    else:
        main_program(opts)
            

    
