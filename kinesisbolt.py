import boto
import errno
from socket import error as SocketError
from boto.kinesis.exceptions import ProvisionedThroughputExceededException
import time
from bolt import SpoutBase, BoltBase
import collections
import logging
import json
import msgpack
import base64
import msgmodel
import concurrent.futures
import Queue
from streaming.util import TCounter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException, LimitExceededException
from boto.exception import JSONResponseError


class KinesisSpout(SpoutBase):
    _DEFAULT_GET_CHUNK_SIZE = 150
    _DEFAULT_POLLING_INTERVAL = 4.00
    def __init__(self, stream, shard_id, *args, **kwargs):
        super(KinesisSpout, self).__init__(*args, **kwargs)
        self._kin = boto.connect_kinesis()
        self._log = kwargs.get('logger', logging.getLogger("__main__"))
        self._set_shard_info(stream, shard_id)
        self._log.info("Shard INFO: %s" % str(self._shard_details))
        self._last_seqnum = self._get_last_seqnum(**kwargs)
        self._current_seqnum = self._last_seqnum
        self._last_fetched_seqnum = None
        self._last_seqnum_cb = None
        self._shard_iter = None
        self._rec_chunk = kwargs.get('chunk_size', self._DEFAULT_GET_CHUNK_SIZE)
        self._polling_interval = kwargs.get('polling_interval', self._DEFAULT_POLLING_INTERVAL)
        self._throttle_interval = kwargs.get('throttle_interval', (self._polling_interval/2))
        self._eob_cb = kwargs.get('eob_cb')
        self._cntr = kwargs.get('cntr', TCounter())
        self._max_retries = 3
        self._last_req_time = time.time()
        self._rec_buff = collections.deque()
        self._init_time = time.time()
        self._start_at = kwargs.get("start_at", "LATEST")
        self._fetch_count = 0
        self._log.info("Finished init of stream: {0} : {1}    LastSeqnum: {2}".format(stream, self._shard_id, self._last_seqnum))

    def _set_shard_info(self, stream, shard_id):
        stream_info = self._kin.describe_stream(stream)
        self._kstream = stream_info['StreamDescription']
        self._shard_id = shard_id or self._kstream['Shards'][0]['ShardId']
        self._shard_details = None
        for si in self._kstream['Shards']:
            if (si['ShardId'] == shard_id):
                self._shard_details = si
        if (not self._shard_details):
            raise RuntimeError("shard_id {0} invalid for this stream".format(shard_id))

    def _get_last_seqnum(self, **kwargs):
        if (kwargs.get('last_seqnum')):
            self._log.info("Explicitly setting KinesisSpout to seqnum={0}".format(kwargs['last_seqnum']))
        return kwargs.get('last_seqnum') or self._shard_details['SequenceNumberRange']['StartingSequenceNumber']

    def _set_stream_iterator(self):
        if (self._start_at == 'RESUME'):
            self._log.warn('Resuming stream read @ seqnum={0}'.format(self._last_seqnum))
            rsi = self._kin.get_shard_iterator(self._kstream['StreamName'],
                                               self._shard_id,
                                               'AT_SEQUENCE_NUMBER',
                                               starting_sequence_number=self._last_seqnum)
        else:
            self._log.warn('Starting stream read @ {0}'.format(self._start_at))
            rsi = self._kin.get_shard_iterator(self._kstream['StreamName'],
                                               self._shard_id,
                                               self._start_at)
        self._shard_iter  = rsi['ShardIterator']

    def _get_fresh_shard_iterator(self, **kwargs):
        seqnum = kwargs.get('seqnum', self._last_fetched_seqnum)
        start_at = 'AT_SEQUENCE_NUMBER'
        if (seqnum is None):
            kw = {}
            start_at = 'LATEST'
        else:
            kw = dict(starting_sequence_number=seqnum)
        self._log.warn('Fresh shard iterator   stream = {1}  shard = {2}  seqnum = {0}  start_at = {3}'.
                       format(seqnum, self._kstream['StreamName'], self._shard_id, start_at))
        rsi = self._kin.get_shard_iterator(self._kstream['StreamName'],
                                           self._shard_id,
                                           start_at,
                                           **kw)
        self._log.warn("Old shard iterator: {0}   New shard iterator {1}".format(self._shard_iter, rsi['ShardIterator']))
        self._shard_iter  = rsi['ShardIterator']

    def unpack_to(self, payload, seqnum, recbuffer):
        recbuffer.append((seqnum, payload))

    @property
    def current_seqnum(self):
        return self._current_seqnum

    def kinesis_get_cb(self, record_bundle):
        pass

    def raw_record_process(self, kinesis_record):
        bb = base64.b64decode(kinesis_record['Data'])
        kinesis_record['Data'] = bb

    def get_next_event(self, nowait=False):
        if (self._shard_iter is None):
            self._set_stream_iterator()
        if ((self._eob_cb) and (self._current_seqnum != self._last_seqnum_cb) and (len(self._rec_buff) == 0)):
            self._last_seqnum_cb = self._current_seqnum
            self._eob_cb(self._current_seqnum)
                
        while (len(self._rec_buff) == 0):
            delta = time.time() - self._last_req_time
            if (delta < self._throttle_interval):
                self._cntr.incr("kthrottle")
                time.sleep((self._throttle_interval - delta))
            self._last_req_time = time.time()
            res = None
            tf0 = time.time()
            for retry in xrange(self._max_retries):
                try:
                    res = self._kin.get_records(self._shard_iter, limit=self._rec_chunk, b64_decode=False)
                except TypeError as e:
                    self._log.exception('kinesis get records')
                    self._log.warn('trapped error on Kinesis, attempt {0}/{1}  {2}'.format(retry, self._max_retries, e))
                except ProvisionedThroughputExceededException as e:
                    self._log.warn("Exceeded throughput limits.. throttling")
                    time.sleep(self._throttle_interval)
                except SocketError as e:
                    if (e.errno != errno.ECONNRESET):
                        self._log.exception('Socket exception  {0}'.format(str(e)))
                        self._log.warn("Socket exception")
                        raise
                    self._log.warn("Socket connection error retry {0}/{1}".format(retry, self._max_retries))
                    time.sleep(self._throttle_interval)
                except Exception as e:
                    self._log.exception('Unknown exception  {0}'.format(str(e)))
                    self._log.warn("Unknown exception")
                    raise
                else:
                    break
            tfetch = time.time() - tf0
            self._fetch_count += 1
            self._cntr.incr("kfetch")
            if (not res):
                self._log.warn('MAX retries on Kinesis... sleeping then fresh iterator from last fetched seqnum: {0}'.format(self._last_fetched_seqnum))
                time.sleep(self._polling_interval*5)
                self._get_fresh_shard_iterator()

            else:
                self._rec_buff = collections.deque()
                if ('Records' in res):
                    if (nowait) and (len(res["Records"]) == 0):
                        return {}
                    self.kinesis_get_cb(res)
                    self._shard_iter = res['NextShardIterator']
                    for (irec, recs) in enumerate(res['Records']):
                        self.raw_record_process(recs)
                        self.unpack_to(recs['Data'], recs['SequenceNumber'], self._rec_buff)
                        self._last_fetched_seqnum = recs['SequenceNumber']
                    if (len(res['Records']) == self._rec_chunk): self._cntr.incr('kmaxrecget')
                    if (len(res['Records']) == 0): self._cntr.incr("kempty")

                    if (len(self._rec_buff) and ((self._fetch_count % 50) == 0)):
                        ev0 = self._rec_buff[0][1]
                        if ev0.get('timestamp'):
                            self._log.info("Timestamp: {0}   CNTRS: {1}".format(ev0['timestamp'], self._cntr.pprint()))
                        else:
                            self._log.info("Timestamp: {0}   CNTRS: {1}".format(ev0['unix_timestamp'], self._cntr.pprint()))
                        if (self._cntr["kfetch"]>1000):
                            self._log.info("Counters reset")
                            self._cntr.reset_all()
                    self._log.debug('Record count: {1}   Next Shard Iterator: {0}'.format(res['NextShardIterator'], len(self._rec_buff)))

                if (len(self._rec_buff) == 0):
                    telap = time.time() - self._last_req_time
                    if (telap < self._polling_interval):
                        self._cntr.incr("kbackoff")
                        time.sleep(self._polling_interval - telap + 0.001)

        (seqnum, rec) = self._rec_buff.popleft()
        self._current_seqnum = seqnum
        return rec




class KinesisSpoutUnbundler(KinesisSpout):
    def __init__(self, *args, **kwargs):
        super(KinesisSpoutUnbundler, self).__init__(*args, **kwargs)

    def unpack_to(self, payload, seqnum, recbuffer):
        recbundle = []
        try:
            recbundle = msgpack.unpackb(payload)
        except msgpack.exceptions.ExtraData as ed:
            self._log.warn("Msgpack Unpack ExtraData, Recovered RecLen={0}".format(str(ed.unpacked)))
        except Exception as e:
            self._log.warn("Msgpack Unpack Exception {0}".format(type(e)))
        self._log.debug("bundle count: {0}".format(len(recbundle)))
        for rec in recbundle:
            recbuffer.append((seqnum, msgmodel.expand(rec)))

    
    



class KinesisSink(BoltBase):
    def __init__(self, stream, *args, **kwargs):
        super(KinesisSink, self).__init__(*args, **kwargs)
        self._stream = stream
        self._kin = boto.connect_kinesis()
        stream_info = self._kin.describe_stream(stream)
        self._kstream    = stream_info['StreamDescription']
        self._keyname    = kwargs.get('keyname', None)
        self._setpartkey = kwargs.get('setpartkey', (lambda evt: evt[self._keyname]))
        self._cntr = kwargs.get('cntr', TCounter())
        self._count = 0
        self._log = kwargs.get('logger', logging.getLogger("__main__"))
        self._tpause = 0.05
        self._ksplit = 0
        self._dropped = 0

    def pack(self, record):
        return msgpack.packb(record)

    def kin_send(self, in_record):
        vrecords = [ in_record ]
        for xplit in xrange(2):
            for record in vrecords:
                precord = self.pack(record)
                kerr = 0
                for itry in xrange(3):
                    resp = None
                    try:
                        resp = self._kin.put_record(self._stream, precord, self._setpartkey(record))
                    except (ProvisionedThroughputExceededException, TypeError) as ev:
                        self._log.warn('Exceeded sink throughput... pausing')
                        time.sleep(self._tpause)
                        kerr = 1
                    except (LimitExceededException, JSONResponseError) as ev:
                        kerr = 2
                        break
                    except Exception as e:
                        self._log.warn('Exception Kinesis put {0}'.format(str(type(e))))
                        return None
                    else:
                        kerr = 0
                        if ((self._count % 5000) == 0):
                            self._log.warn("Put record: {0}   Seq: {1}".format(self._count, resp['SequenceNumber']))
                        break

                if (kerr == 1):
                    self._log.warn('Maximum retries on throughout... dropping record')
                    self._dropped += 1
                    return None
                elif (kerr == 2):
                    break
            
            if (not kerr):
                return resp

            self._ksplit += 1
            self._log.info("Kinesis send record split: {0}   {1}:{2}".format(len(in_record), self._ksplit, self._count))
            xl2 = len(in_record)/2
            vrecords = [in_record[:xl2], in_record[xl2:]]
        return None
    
    def process(self, record):
        self._count += 1
        self.kin_send(record)
        



def ftwrap(targetobj, method, *args):
    return getattr(targetobj, method)(*args)


class KinesisSinkAsync(KinesisSink):
    _MAX_WORKERS = 2
    def __init__(self, *args, **kwargs):
        super(KinesisSinkAsync, self).__init__(*args, **kwargs)
        self._maxworkers = kwargs.get('maxworkers', KinesisSinkAsync._MAX_WORKERS)
        self._thrpool = kwargs.get('thrpool', concurrent.futures.ThreadPoolExecutor(max_workers=self._maxworkers))
        self._futures = []
        self._sendq = Queue.Queue()
        self._maxquelen = 500
        self._pauselen = 10

    def send_chain(self):
        while (True):
            try:
                record = self._sendq.get(False)
            except Queue.Empty:
                break
            else:
                self.kin_send(record)
        return True

    def _reap_futures(self):
        ftv = self._futures
        self._futures = []
        for ft in ftv:
            if (not ft.done()):
                self._futures.append(ft)

    def _check_queue(self):
        if (self._sendq.qsize() > self._pauselen):
            time.sleep(0.005)
        trim_count = 0
        while (self._sendq.qsize() > self._maxquelen):
            try:
                rtrash = self._sendq.get(False)
            except Queue.Empty:
                break
            else:
                trim_count += 1
                self._cntr.incr('qtrim')
            time.sleep(0.010)
        if (trim_count):
            self._log.warn("Queue Too Long trimmed: {0} -- Please rescale Kinesis stream".format(trim_count))
        if (self._sendq.qsize() > self._pauselen):
            time.sleep(0.005)

    def process(self, record):
        self._count += 1
        self._check_queue()
        self._sendq.put(record)
        #
        self._reap_futures()
        if ((self._count % 5000) == 0):
            self._log.info("{0}  SendQ: {1}".format(self._count, self._sendq.qsize()))
        #
        if (len(self._futures) < self._maxworkers):
            ft = self._thrpool.submit(ftwrap, self, 'send_chain')
            self._futures.append(ft)

                
        
    




if __name__ == "__main__":

    from tools import energeia
    import random

#    boto.connect_kinesis = (lambda: energeia.EnergeiaClient())

    class DebugBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(DebugBolt, self).__init__(*args, **kwargs)
            self._count = 0

        def process(self, data):
            self._count += 1
            if ((self._count % 10000) == 0):
                logging.info('Records recieved {0}'.format(self._count))
            return data

        def shutdown(self):
            print 'Shutdown called'
            pass
    
    db = DebugBolt()
#    ksink = KinesisSink("AdsQueryCoalesced", keyname="impression_id")
    opts = dict(last_seqnum="49537335845514728365598234995297135798378439738109460513")
    kk = KinesisSpoutUnbundler("AdsQueryRaw", "shardId-000000000002", **opts)
#    kk.addsink(ksink)
    kk.addsink(db)
    kk.run()
