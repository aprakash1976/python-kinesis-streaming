import re
import sys
import logging
import time
import os
import calendar
import hashlib
import argparse
from datetime import datetime, timedelta
from streaming.bolt import BoltBase, BoltTerminate
from affine.log import configure_logging
from streaming.kinesisbolt import KinesisSpoutUnbundler, KinesisSink
import boto

roll_log = logging.getLogger('lineitemrollup')


class StreamInfo(object):
    def __init__(self, streamname, *args, **kwargs):
        skinconn = boto.connect_kinesis()
        strinfo = skinconn.describe_stream(streamname)
        skinconn.close()
        self._sinfo = strinfo['StreamDescription']
        self._shardwidth = len(self._sinfo['Shards'])
        self._shhi = [int(sh['HashKeyRange']['EndingHashKey']) for sh in self._sinfo['Shards']]

    def getshardindex(self, shardkey):
        ival = int(hashlib.md5(shardkey).hexdigest(), 16)
        assert((ival <= self._shhi[-1]))
        for shidx in xrange(self._shardwidth):
            if (ival <= self._shhi[shidx]):
                return shidx
        


class DebugBolt(BoltBase):
    def __init__(self, *args, **kwargs):
        super(DebugBolt, self).__init__(*args, **kwargs)
        self._cnt = 0
        self._log = kwargs.get("logger", logging.getLogger("__main__"))
        self._qlin = set((4384, ))
#        self._qlin = set((4384, 4385, 4386, 4387))
        self._ccnt = 0
        self._lcnt = 0
        self._started = False
        self._streaminfo = kwargs.get("streaminfo")
        self._shardindex = kwargs.get("shardindex",0)

    def _start(self):
        if (not self._started):
            self._t0 = time.time()
        self._started = True

    def process(self, imp):
        self._start()
        if ('video_id' not in imp):
            self._cnt += 1
            print imp.get('page_id')
            if (self._cnt > 1000):
                raise BoltTerminate

    

if __name__ == "__main__":
    aparse = argparse.ArgumentParser()
    aparse.add_argument('--idx', type=int, required=True)
    aparse.add_argument('stream')
    opts = aparse.parse_args()

    #streamname = (len(sys.argv)>1) and sys.argv[1] or "AdsQueryCoalesced"
    #kinspout = KinesisSpoutUnbundler('AdsQueryRaw', 'shardId-000000000000')
    si = StreamInfo(opts.stream)

    shardid = 'shardId-000000000{0:03d}'.format(opts.idx)
    print "Checking shard : ", shardid
    debugbolt = DebugBolt(streaminfo=si, shardindex=opts.idx)
    kinspout = KinesisSpoutUnbundler(opts.stream, shardid)
    kinspout.addsink(debugbolt)
    try:
        kinspout.run()
    except KeyboardInterrupt:
        kinspout.shutdown()
        raise
    except BoltTerminate:
        pass
