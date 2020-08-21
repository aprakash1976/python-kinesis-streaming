from datetime import datetime
from affine.normalize_url import parse_url
from bolt import BoltBase, SpoutBase, DemuxBase
from streaming.util import TimeWinStore, TCounter, args_parser, IntervalDispatcher
import affine.config
import copy
import logging
import hashlib
import random
import daemon
import time
import os
import threading
import calendar
import pylru
import sys
import signal
import boto
import platform
from hashlib import md5
from streaming import msgmodel
from daemon.pidfile import TimeoutPIDLockFile
from affine.log import configure_logging
from affine.uuids import generate_id
from kinesisboltstate import KinesisSpoutUnbundlerState
from kinesisbolt import KinesisSinkAsync, KinesisSink
from affine.set_librato import SetLibrato

args_cfg = {
    "idx" : {
        "tovalue" : int
    },
    "start": {
        "tovalue" : str
    }
}  


def nextpow2(x):
    for y in map(lambda z: 1<<z, xrange(32)):
        if (y >= x): return y
    return None


class AdServCoalesce(BoltBase):
    _VIEWABILITY_LABEL_ID = 7840
    _DEFAULT_COALESCE_WINDOW = 480
    _LIBRATO_PREFIX = "streaming.coalesce."
    def __init__(self, *args, **kwargs):
        super(AdServCoalesce, self).__init__(*args, **kwargs)
        self._window_time = kwargs.get('window_time', self._DEFAULT_COALESCE_WINDOW)
        self._evt_count = 0
        self._evt_counter = 0
        self._evt_lag = 0
        self._epoch = datetime.utcfromtimestamp(0)
        self._twstore = TimeWinStore(self._window_time, handler=self)
        self._log = logging.getLogger("__main__")
        self._started = False
        self._cntr = TCounter()
        self._copy_keys = ('media_partner_name', 'line_item_responses', 'true_label_ids', 
                           'request_id', 'page_id', 'url', 'user_id', 'video_id',
                           'country_code', 'demo_id', 'app', 'app_id')
        self._last_time_str = ""
        self._last_ts = None
        self._url_cache = pylru.lrucache(200000)
        self._print_granul = kwargs.get('print_granul', None)
        env = affine.config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._libr_client = SetLibrato(affine.config, prefix + self._LIBRATO_PREFIX)
        self._libr_source = platform.node()
        self._treporter = IntervalDispatcher(600, self, '_libr_reporter', daemon=True)
        self._REPORTING_COUNTERS = {'qtrim': 'queue_size', 'elag': 'event_lag'}

    def _libr_reporter(self):
        if (self._evt_counter == 0): return
        self._cntr['elag'] = float(self._evt_lag)/self._evt_counter
        gauge_opts = dict(measure_time=int(time.time()), source=self._libr_source)
        lmetric = {}
        for (cntr,label) in self._REPORTING_COUNTERS.iteritems():
            lmetric[label] = dict(gauge=self._cntr[cntr])
            lmetric[label].update(gauge_opts)
            self._cntr.set(cntr, 0)
        self._libr_client.bulk_gauge(lmetric)
        self._evt_counter = 0
        self._evt_lag = 0

    def _start(self):
        if (not self._started):
            self._started = True
            self._treporter.dispatch()

    def _coalesce(self, join_key, imp_line_item_id, ts, evt):
        if (join_key in self._twstore):
            imp = self._twstore[join_key]
            etype = evt.get('event_type', None)
            if (etype):
                imp['event_list'].append((etype, ts))
                if (imp_line_item_id) and ('imp_line_item_id' not in imp) and (etype != 'query'):
                    imp['imp_line_item_id'] = imp_line_item_id
                if (etype == 'query'):
                    self._cntr.incr("queryafter")
                    for ckey in self._copy_keys:
                        if (ckey in evt):
                            imp[ckey] = evt[ckey]
            else:
                self._cntr.incr("noevttype")

    def process(self, evt):
        self._start()
        self._cntr.incr('event')
        self._evt_count += 1
        self._evt_counter += 1
        if ((self._print_granul) and (self._evt_count%self._print_granul) == 0):
            self.loginfo()
        if (evt['timestamp'] != self._last_time_str):
            self._last_time_str = evt['timestamp']
            dtime = datetime.strptime(evt["timestamp"], "%Y-%m-%d %H:%M:%S")
            self._last_ts = calendar.timegm(dtime.timetuple())
	    now_time = datetime.now()
	    nstime = now_time.strftime("%Y-%m-%d %H:%M:%S")
	    nstrtime = datetime.strptime(nstime, "%Y-%m-%d %H:%M:%S")
	    current_ts = calendar.timegm(nstrtime.timetuple())
            self._evt_lag += current_ts - self._last_ts
        if (evt.get('impression_id') is None):
            self._cntr.incr("noimp")
            return None
        if (evt.get('event_type') is None):
            self._cntr.incr("noetype")
            self._log.info('malformed event: {0}'.format(str(evt)))
            return None
        if (evt.get('url')):
            uhash = md5(evt["url"]).hexdigest()
            self._cntr.incr("urllookup")
            if (uhash not in self._url_cache):
                self._cntr.incr("urlmiss")
                self._url_cache[uhash] = parse_url(evt['url'])
            evt['url'] = self._url_cache[uhash]

        self._log.debug('Evt Count: %d  Imp: %s' % (self._evt_count, evt['impression_id']))
        if (evt['impression_id'].count("_") != 1):
            self._cntr.incr("badimp")
            return None
        (join_key, imp_line_item_id) = evt['impression_id'].split("_", 1)
        if (join_key in self._twstore):
            self._coalesce(join_key, imp_line_item_id, self._last_ts, evt)
        else:
            self._cntr.incr("newevt")
            evt = copy.copy(evt)
            etype = evt['event_type']
            evt['unixtime'] = self._last_ts
            mxelag = max(self._cntr['elag'], int(time.time()) - self._last_ts)
            self._cntr.set('elag', mxelag)
            evt['event_list'] = [(etype, self._last_ts)]
            if (etype != 'query'):
                evt['imp_line_item_id'] = imp_line_item_id
            del evt['event_type']
            oob = self._twstore.timed_add(self._last_ts, join_key, evt)
            if (oob):
                self._cntr.incr("oob")
        return None

    def _add_fake_meta(self, imp):
        imp['url'] = "http://www.set.tv/fakemeta"
        imp['true_label_ids'] = [self._VIEWABILITY_LABEL_ID]

    def purge(self, evictions):
        self._log.debug('Flushing %d impressions' % len(evictions))
        for (imp_id, imp) in evictions:
            if (not imp.get('url')):
                self._add_fake_meta(imp)
                self._cntr.incr("nometa")
            self._dispatch(imp)
        return

    def loginfo(self):
        self._log.info("counters: {0}".format(",".join((k+':%d'%self._cntr[k] for k in self._cntr.keys()))))
        self._cntr.reset_all()

    def shutdown(self):
#        self._tinfolog.cancel()
        pass

        

class ShardBundler(BoltBase):
    # python estimate of Kinesis 500kB put record limit
    _MAX_KINESIS_PUT_LIMIT = 280000
    def __init__(self, keyname, bundlesize, streaminfo, *args, **kwargs):
        super(ShardBundler, self).__init__(*args, **kwargs)
        self._keyname    = keyname
        self._bundlesize = bundlesize
        self._streaminfo = streaminfo
        self._shardwidth = len(streaminfo['Shards'])
        self._bundles    = [ list() for x in xrange(self._shardwidth) ]
        self._bsizes     = [0]*self._shardwidth
        self._shhi       = []
        self._bcnt       = 0
        self._log = logging.getLogger("__main__")
        self._setpartkey = kwargs.get('setpartkey')
        for shidx in xrange(self._shardwidth):
            sh = streaminfo['Shards'][shidx]
            (lo, hi) = (int(sh['HashKeyRange']['StartingHashKey']), int(sh['HashKeyRange']['EndingHashKey']))
            self._shhi.append(hi)

        def lrsize(x):
            s = 72 + nextpow2(len(x))*8
            return s + sum(((238,182)[len(t)==3] for t in x))
        self._esizes = {
            'el' : lambda x: len(x)*155 + 74,
            'lr' : lrsize,
            'tl' : lambda x: len(x)*32 + 74
            }

    def _shardindex(self, shardkey):
        ival = int(hashlib.md5(shardkey).hexdigest(), 16)
        assert((ival <= self._shhi[-1]))
        for shidx in xrange(self._shardwidth):
            if (ival <= self._shhi[shidx]):
                return shidx

    def _evt_size_est(self, evt):
        s = 1024
        for (k,v) in evt.items():
            s += 37 + len(k)
            if (k in self._esizes):
                s += self._esizes[k](v)
            elif (isinstance(v, str)):
                s += 37 + len(v)
            else:
                s += 24
        return s

    def process(self, evt):
        if (evt.get(self._keyname, None) is None): return
        self._bcnt += 1
        partkey = (self._setpartkey) and self._setpartkey(evt) or evt[self._keyname]
        sidx = self._shardindex(partkey)
        compact_evt = msgmodel.compact(evt)
        esize = self._evt_size_est(compact_evt)
        if ((self._bsizes[sidx] + esize) > self._MAX_KINESIS_PUT_LIMIT) or (len(self._bundles[sidx])>80):
            self._log.debug("IDX: {2} Rec Count: {0}  Bundle Size: {1}".format(len(self._bundles[sidx]), self._bsizes[sidx], sidx))
            bundle = copy.copy(self._bundles[sidx])
            self._dispatch(bundle)
            del self._bundles[sidx][:]
            self._bsizes[sidx] = 0
        self._bundles[sidx].append(compact_evt)
        self._bsizes[sidx] += esize



if __name__ == "__main__":
    from mpkinesismgr import MPKinesisMgr

    class AdservCoalesceMgr(MPKinesisMgr):
        def __init__(self, processname, *args, **kwargs):
            super(AdservCoalesceMgr, self).__init__(processname, *args, **kwargs)

        def TERM_handler(self, signum):
            self._log.warn("caught TERM signal .. shutting down")
            self._sink.shutdown()
            self._worker.shutdown()
            self._spout.shutdown()
            sys.exit(0)

        def main(self):
            (aoptions, otherargs) = args_parser(sys.argv[1:], args_cfg)
            configure_logging("{0}_{1:04d}.log".format(self._name, int(aoptions.get('idx', os.getpid()))))
            start_at = aoptions.get("start", "RESUME")
            if (start_at not in ("RESUME", "LATEST", "TRIM_HORIZON")):
                self._log.warn("Illegal kinesis start position: {0}".format(start_at))
                return None
            self._log = logging.getLogger("__main__")
            self._log.info("inside main: pid={0} idx={1}".format(os.getpid(), aoptions['idx']))
            self._log.info("options: {0}".format(aoptions))
            self._log.info("otherargs: {0}".format(otherargs))
            self._log.info("has TERM handler {0}".format(signal.getsignal(signal.SIGTERM)))
            streams = filter((lambda a: not a.startswith("-")), otherargs)
            self._streamname = streams[0]
            self._set_streaminfo()
            self._shard = self._shards[aoptions['idx']]
            self._log.info("Shard info: {0}".format(self._shard))
            skinconn = boto.connect_kinesis()
            sink_info = skinconn.describe_stream(streams[1])['StreamDescription']
            skinconn.close()
#
#  older sharding via URLs  (switched to impression id)
#            self._bundler   = ShardBundler('url', 50, sink_info, setpartkey=(lambda rec: rec['url'][:60]))
#            self._sink      = KinesisSinkAsync(streams[1], setpartkey=(lambda rec: rec[0]['url'][:60]))
#
            self._worker = AdServCoalesce()
            self._bundler   = ShardBundler('url', 50, sink_info, setpartkey=(lambda rec: rec['impression_id'].split("_")[0]))
            self._sink      = KinesisSinkAsync(streams[1], cntr=self._worker._cntr, 
                                               setpartkey=(lambda rec: rec[0]['ii'].split("_")[0]))
            self._bundler.addsink(self._sink)
            self._worker.addsink(self._bundler)
            self._spout = KinesisSpoutUnbundlerState(self._streamname, self._shard['ShardId'], 'coalesce', 
                                                     logger=self._log, cntr=self._worker._cntr, 
                                                     start_at=start_at)
            self._spout.addsink(self._worker)
            self._spout.run()
            self._log.info("Main exit")
            return None

    t = AdservCoalesceMgr("adservcoalesce")
    t.start()
    sys.exit(0)

