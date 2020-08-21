import sys
import boto
import time
from Queue import Queue
from datetime import datetime, timedelta
from threading import Thread
from collections import defaultdict
import io
import msgpack
import os.path
import logging
import argparse
from streaming.einterposer import Bundler, TSSeq
from boto.sqs.message import Message


def snap_minute(tnow, intvl):
    return datetime(*tnow.timetuple()[:4]) + timedelta(minutes=(tnow.minute-(tnow.minute % intvl)))


class CBundler(Thread):
    def __init__(self, streamname, logger=None):
        super(CBundler, self).__init__()
        self.daemon = True
        self._bundler = Bundler(streamname, efilter=(lambda r: 'ii' in r),
                                setpartkey=(lambda r: r['ii'].split("_")[0]),
                                logger=logger)
        self._fifo = Queue()

    def getfifo(self):
        return self._fifo

    def run(self):
        while (True):
            r = self._fifo.get(True)
            r['seq'] = TSSeq(r['seq'])
            self._bundler.add(r)


class S3EvtmsgReader(Thread):
    def __init__(self, bucketname, keyname, rate, fifo):
        super(S3EvtmsgReader, self).__init__()
        bucket = boto.connect_s3().get_bucket(bucketname)
        self._s3key = bucket.get_key(keyname)
        self._elap  = 1.0/float(rate)
        self._fifo  = fifo

    def run(self):
        wc = time.time()
        eclock = None
        last_ts = ''
        tt0 = time.time()
        blob = self._s3key.get_contents_as_string()
        self._s3key.close()
        if (not blob):
            return
        rcnt = 0
        iobuf = io.BytesIO(blob)
        unpacker = msgpack.Unpacker()
        while (True):
            chunk = iobuf.read(2048)
            if (len(chunk) == 0):
                break
            unpacker.feed(chunk)
            for rec in unpacker:
                ts = rec['record']['unix_timestamp']
                if (ts != last_ts):
                    td = datetime.utcfromtimestamp(ts)
                    if (eclock is None):
                        eclock = td
                    if (td > eclock):
                        edel = ((td - eclock).total_seconds())*self._elap
                        tsleep = edel-(time.time()-wc)
                        eclock = td
                        if (tsleep > 0):
                            time.sleep(tsleep)
                        wc = time.time()
                    last_ts = ts
                rcnt += 1
                self._fifo.put(rec, False)
        print rcnt, time.time() - tt0


def s3stepper(tstart, tend):
    dt = snap_minute(tstart, 5)
    while (dt <= tend):
        keypref = "ads_servers/events2/{0:02d}_{1:02d}/{2:02d}/{3:02d}/{4:02d}/".\
                  format((dt.year-2000), dt.month, dt.day, dt.hour, dt.minute)
        yield keypref
        dt += timedelta(minutes=5)


def get_quorum(qs, count, prefix):
    slaves = set()
    while (len(slaves) < count):
        mv = qs.get_messages(wait_time_seconds=2, visibility_timeout=1)
        for m in mv:
            b = m.get_body()
            if (b.startswith(prefix)):
                slave_idx = int(b.split(" ")[1])
                slaves.add(slave_idx)
                qs.delete_message(m)
    print "Quorum count=",len(slaves), "prefix=", prefix
    return slaves


def node_alloc(qs, count):
    node_place = defaultdict(list)
    for (ii,node) in enumerate(get_nodes(tstart)):
        node_place[(ii%count)].append(node)
    for idx in xrange(count):
        m = Message()
        m.set_body("node {0} {1}".format(idx, " ".join(node_place[idx])))
        qs.write(m)
    get_quorum(qs, count, "allocd")


def step_time_interval(qs, count, tstart, tend):
    for s3dir in s3stepper(tstart, tend):
        t0 = time.time()
        print "Time slot ", s3dir

        for idx in xrange(count):
            m = Message()
            m.set_body("dir {0} {1}".format(s3dir, idx))
            qs.write(m)
        get_quorum(qs, count, "scanned")
        print "Elapsed : {0:.2f}s".format(time.time() - t0)
    print "Sending quit message"
    m = Message()
    m.set_body("quit")
    qs.write(m)


def get_nodes(tnow):
    tnow = snap_minute(tnow, 5)
    keypref = "ads_servers/events2/{0:02d}_{1:02d}/{2:02d}/{3:02d}/{4:02d}/".\
              format((tnow.year-2000), tnow.month, tnow.day, tnow.hour, tnow.minute)
    bucket_name = "affine-streaming-data"
    s3_conn = boto.connect_s3()
    bucket = s3_conn.get_bucket(bucket_name)
    return [os.path.basename(s3key.name) for s3key in bucket.list(prefix=keypref)]


def slave_do_work(qs, streamname, myidx, count):
    not_quit = True
    consumers = {}
    while (not_quit):
        mv = qs.get_messages(wait_time_seconds=10, visibility_timeout=1)
        for m in mv:
            b = m.get_body()
            if (b == 'quit'):
                not_quit = False
                break
            elif (b.startswith("node")):
                (dummy, nidx, nodelist) = b.split(" ", 2)
                if (int(nidx) == myidx):
                    qs.delete_message(m)
                    nodev = nodelist.split(" ")
                    for node in nodev:
                        consumers[node] = CBundler(streamname, logger=logger)
                        consumers[node].start()
                    m = Message()
                    m.set_body("allocd {0}".format(myidx))
                    qs.write(m)
            elif (b.startswith("dir")):
                (dummy, s3dir, idx) = b.split(" ")
                if (int(idx) == myidx):
                    qs.delete_message(m)
                    scan_dir(consumers, s3dir, count)
                    m = Message()
                    m.set_body("scanned {0}".format(myidx))
                    qs.write(m)
    print "Slave exiting"


def scan_dir(consumers, s3dir, count):
    bucket_name = "affine-streaming-data"
    s3_conn = boto.connect_s3()
    bucket = s3_conn.get_bucket(bucket_name)
    treaders = []
    for s3key in bucket.list(prefix=s3dir):
        node = os.path.basename(s3key.name)
        if (node in consumers):
            print s3dir, node
            t0 = S3EvtmsgReader(bucket_name, s3key.name, 10, consumers[node].getfifo())
            t0.start()
            treaders.append(t0)
        s3key.close()
        del s3key
    for t0 in treaders:
        t0.join()


if __name__ == "__main__":
    aparse = argparse.ArgumentParser()
    aparse.add_argument('--idx', type=int)
    aparse.add_argument('--count', type=int, required=True)
    aparse.add_argument('--start', type=str)
    aparse.add_argument('--end', type=str)
    aparse.add_argument('--sqs', type=str)
    aparse.add_argument('stream')
    opts = aparse.parse_args()
    if ((opts.start is None) or (opts.end is None)) and (opts.idx is None):
        sys.stderr.write("Usage error\n")
        sys.exit(1)
    if opts.start:
        tstart = datetime.strptime(opts.start, "%Y-%m-%d %H:%M")
    if opts.end:
        tend = datetime.strptime(opts.end, "%Y-%m-%d %H:%M")
    if opts.sqs:
        sqsname = opts.sqs
    else:
        sqsname = "kbackfill"
    sqs_conn = boto.connect_sqs()
    qs = sqs_conn.get_queue(sqsname)
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    if (qs is None):
        sqs_conn.create_queue(sqsname, 20*60)
        qs = sqs_conn.get_queue(sqsname)
    if (opts.idx is None):
        qs.purge()
        slaves = get_quorum(qs, opts.count, "slave")
        node_alloc(qs, opts.count)
        step_time_interval(qs, opts.count, tstart, tend)
    else:
        m = Message()
        m.set_body("slave {0}".format(opts.idx))
        qs.write(m)
        slave_do_work(qs, opts.stream, opts.idx, opts.count)
    sys.exit(0)
