import os
import logging
import sys
import time
import signal
import hashlib
import copy
from streaming import msgmodel
from streaming.util import args_parser
from streaming.kinesisbolt import KinesisSpoutUnbundler
from kinesisbolt import KinesisSinkAsync
from streaming.bolt import BoltBase
from streaming.mpkinesismgr import MPKinesisMgr
from affine.log import configure_logging

args_cfg = {
    "idx" : {
        "tovalue" : int
        },
    }



class ShardBundler(BoltBase):
    def __init__(self, keyname, bundlesize, streaminfo, *args, **kwargs):
        super(ShardBundler, self).__init__(*args, **kwargs)
        self._keyname    = keyname
        self._bundlesize = bundlesize
        self._streaminfo = streaminfo
        self._shardwidth = len(streaminfo['Shards'])
        self._bundles    = [ list() for x in xrange(self._shardwidth) ]
        self._shhi       = []
        self._bcnt       = 0
        self._log        = kwargs.get('logger', logging.getLogger("__main__"))
        for shidx in xrange(self._shardwidth):
            sh = streaminfo['Shards'][shidx]
            (lo, hi) = (int(sh['HashKeyRange']['StartingHashKey']), int(sh['HashKeyRange']['EndingHashKey']))
            self._shhi.append(hi)

    def _shardindex(self, shardkey):
        ival = int(hashlib.md5(shardkey).hexdigest(), 16)
        assert((ival <= self._shhi[-1]))
        for shidx in xrange(self._shardwidth):
            if (ival <= self._shhi[shidx]):
                return shidx

    def process(self, evt):
        if (self._keyname not in evt): return
        pkey = evt['impression_id'].split('_')[0][:60]
        sidx = self._shardindex(pkey)
        compact_evt = msgmodel.compact(evt)
        if (len(self._bundles[sidx]) >= self._bundlesize):
            self._bcnt += 1
            if ((self._bcnt % 1000) == 0):
                r = self._bundles[sidx][0]
                self._log.info("IDX: {1} Rec Count: {0}   Key: {2}".format(len(self._bundles[sidx]), sidx, r.get('ii','__')))
            bundle = copy.copy(self._bundles[sidx])
            self._dispatch(bundle)
            del self._bundles[sidx][:]
        self._bundles[sidx].append(compact_evt)




class BridgeMPMgr(MPKinesisMgr):
    '''
    
    '''
    def __init__(self, processname, *args, **kwargs):
        super(BridgeMPMgr, self).__init__(processname, *args, **kwargs)

    def TERM_handler(self, signum):
        self._log.warn("caught TERM signal .. shutting down")
        self._worker.shutdown()
        self._spout.shutdown()
        sys.exit(0)

    def main(self):
        (aoptions, otherargs) = args_parser(sys.argv[1:], args_cfg)
        configure_logging("{0}_{1:04d}.log".format(self._name, int(aoptions.get('idx', os.getpid()))))
        self._log = logging.getLogger("__main__")
        self._log.info("inside main: pid={0} idx={1}".format(os.getpid(), aoptions['idx']))
        self._log.info("options: {0}".format(aoptions))
        self._log.info("otherargs: {0}".format(otherargs))
        self._log.info("has TERM handler {0}".format(signal.getsignal(signal.SIGTERM)))
        cmdargs = [ a for a in otherargs if (not a.startswith('-')) ]
        self._streamname = cmdargs[0]
        self._sink_streamname = cmdargs[1]
        self._set_streaminfo()
        self._shard = self._shards[aoptions['idx']]
        self._log.info("Shard info: {0}".format(self._shard))
        self._spout = KinesisSpoutUnbundler(self._streamname, self._shard['ShardId'], logger=self._log)
        self._sinkinfo  = self._get_streaminfo(self._sink_streamname)
        self._worker = ShardBundler('impression_id', 50, self._sinkinfo, logger=self._log)
        self._sink = KinesisSinkAsync(self._sink_streamname, setpartkey=(lambda rec: rec[0]['ii'].split("_")[0][:60]))
        self._worker.addsink(self._sink)
        self._spout.addsink(self._worker)
        self._spout.run()
        self._log.info("Main exit")
        return None

if __name__ == "__main__":
    b = BridgeMPMgr("kbridge")
    b.start()
    sys.exit(0)

