import logging
import sys
import random
import os
import time
from streaming.util.mprocmgr import MultiProcMgrBase
from streaming.util import args_parser
from streaming.kinesisbolt import KinesisSpoutUnbundler
from streaming.bolt import BoltBase
from affine.log import configure_logging
import signal
import boto


args_cfg = {
    "idx" : {
        "tovalue" : int
        },
    }


class MPKinesisMgr(MultiProcMgrBase):
    '''
    Base class -- creates 1 process per Kinesis shard
    '''
    def __init__(self, *args, **kwargs):
        super(MPKinesisMgr, self).__init__(*args, **kwargs)
        self._shard_range = None

    def _get_streaminfo(self, streamname):
        skinconn = boto.connect_kinesis()
        try:
            strinfo = skinconn.describe_stream(streamname)
        except:
            self._log.warn('Failed to retrieve stream [{0}]. Aborting'.format(streamname))
            sys.exit(1)
        skinconn.close()
        streaminfo = strinfo['StreamDescription']
        return streaminfo

    def _set_streaminfo(self):
        self._streaminfo = self._get_streaminfo(self._streamname)
        self._shards = sorted(self._streaminfo['Shards'], key=(lambda x: x['ShardId']))
        self._shard_lo = 0
        self._shard_hi = len(self._shards)-1
        if (self._shard_range) and (self._shard_range != 'A'):
            (lo,hi) = map(int,self._shard_range.split("-"))
            self._shard_lo = max(lo, 0)
            self._shard_hi = min(hi, len(self._shards)-1)
        self._log.info("Instance shard range: [{0}:{1}]    Total shards:{2}".format(self._shard_lo, self._shard_hi, len(self._shards)))
        self._cidx2sidx = [ sidx for sidx in xrange(self._shard_lo, self._shard_hi+1) ]
            

    def postdaemon(self):
        configure_logging("{0}.log".format(self._name))
        self._log = logging.getLogger("__main__")
        (opts,otheropts) = args_parser(sys.argv[1:], {"shard_range":{"shortcut":"S"}})
        self._shard_range = opts.get("shard_range")
        self._base_child_args = otheropts[:]
        self._log.info("Base child args: {0}".format(self._base_child_args))
        self._streamname = (a for a in self._base_child_args if (not a.startswith('-'))).next()
        self._log.info("Stream name: {0}".format(self._streamname))
        self._set_streaminfo()

    def childidxrange(self):
        return (self._shard_lo, self._shard_hi)

    def childcount(self):
        return self._shard_hi - self._shard_lo + 1
        
    def childargs(self, child_idx):
        return [ "--idx={0}".format(self._cidx2sidx[child_idx]) ]




class Snooper(BoltBase):
    def __init__(self, *args, **kwargs):
        super(Snooper, self).__init__(*args, **kwargs)
        self._event_count = 0
        self._imp_count = 0
        self._log = kwargs.get("logger", logging.getLogger("__main__"))
        if ("logger" not in kwargs):
            self._log.setLevel(logging.INFO)

    def process(self, imp):
        self._event_count += 1
        if ((self._event_count % 1000) == 0):
            self._log.info("{0} ".format(imp.get("impression_id","_")))
        return (False,None)




class SnooperMPMgr(MPKinesisMgr):
    '''
    Demo -- how to write a multi-process Kinesis consumer
    '''
    def __init__(self, processname, *args, **kwargs):
        super(SnooperMPMgr, self).__init__(processname, *args, **kwargs)

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
        self._streamname = (a for a in otherargs if (not a.startswith('-'))).next()
        self._set_streaminfo()
        self._shard = self._shards[aoptions['idx']]
        self._log.info("Shard info: {0}".format(self._shard))
        self._spout = KinesisSpoutUnbundler(self._streamname, self._shard['ShardId'], logger=self._log)
        self._worker = Snooper(logger=self._log)
        self._spout.addsink(self._worker)
        self._spout.run()
        self._log.info("Main exit")
        return None


class MPShardRangeMgr(MultiProcMgrBase):
    '''
    Base class -- creates 1 process per shard *RANGE*
    '''
    def __init__(self, processname, *args, **kwargs):
        super(MPShardRangeMgr, self).__init__(processname, *args, **kwargs)
        self._shard_range = None

    def _get_streaminfo(self, streamname):
        skinconn = boto.connect_kinesis()
        try:
            strinfo = skinconn.describe_stream(streamname)
        except:
            self._log.warn('Failed to retrieve stream [{0}]. Aborting'.format(streamname))
            sys.exit(1)
        skinconn.close()
        streaminfo = strinfo['StreamDescription']
        return streaminfo

    def _set_streaminfo(self):
        self._streaminfo = self._get_streaminfo(self._streamname)
        all_shards = sorted(self._streaminfo['Shards'], key=(lambda x: x['ShardId']))
        all_cnt = len(all_shards)
        if (self._shard_section == 'A'):
            self._shard_seclo = 0
            self._shard_sechi = all_cnt-1
        else:
            self._shard_seclo = int(self._shard_section.split("-")[0])
            self._shard_sechi = min(int(self._shard_section.split("-")[1]), all_cnt-1)
        self._log.info("Total shards: {0}   Section: {1} - {2}".format(all_cnt, self._shard_seclo, self._shard_sechi))
        self._shards = all_shards[self._shard_seclo:self._shard_sechi+1]
        scnt = len(self._shards)
        slab = scnt//self._pcount + (0,1)[(scnt%self._pcount)!=0]
        self._shard_ranges = []
        for lo in xrange(self._shard_seclo, self._shard_sechi+1, slab):
            self._shard_ranges.append((lo, min((lo+slab-1), self._shard_sechi)))
        self._log.info("Shard ranges: {0}".format(",".join(("{0}-{1}".format(lo,hi) for (lo,hi) in self._shard_ranges))))

    def postdaemon(self):
        configure_logging("{0}.log".format(self._name))
        self._log = logging.getLogger("__main__")
        (opts,otheropts) = args_parser(sys.argv[1:], {"shard_section":{"shortcut":"Q"},"process_count":{"shortcut":"P","tovalue":int}})
        self._log.info("Parent opts: {0}".format(opts))
        self._shard_section = opts.get("shard_section", "A")
        self._pcount = opts.get("process_count", 1)
        self._base_child_args = otheropts[:]
        self._log.info("Base child args: {0}".format(self._base_child_args))
        self._streamname = [a for a in self._base_child_args if (not a.startswith('-'))][-1]
        self._log.info("Stream name: {0}".format(self._streamname))
        self._set_streaminfo()

    def predeamon(self):
        self._get_streaminfo(sys.argv[-1])

    def childidxrange(self):
        return (self._shard_seclo, self._shard_sechi)

    def childcount(self):
        return len(self._shard_ranges)
        
    def childargs(self, child_idx):
        crange = self._shard_ranges[child_idx]
        return [ "-S", "{0}-{1}".format(crange[0], crange[1]) ]



if __name__ == "__main__":
    t = SnooperMPMgr("snooper")
    t.start()
    sys.exit(0)




