from datetime import datetime, timedelta
import threading
import logging
import collections
import os
import json
from affine.aws import s3client
from affine import config
from streaming.plugin import PeriodicPluginBase
from script.generate_qs_traffic import LINE_ITEM_ID, S3_EVENTS_DIR



plugins = [ "checktraffic" ]

class TrafficChecker(PeriodicPluginBase):
    def __init__(self, *args, **kwargs):
        super(TrafficChecker, self).__init__(600, *args, **kwargs)
        self._enabled = True
        self._evt_count = 0
        self._obs_impid = {}
        self._missing = {}
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._wrlock = threading.Lock()
        s3_bucket = config.get('traffic_generator.bucket') or "set-monitor-output"
        self._bucket = s3client.connect(s3_bucket)
        self._s3tstamps = collections.deque()
        self._last_scan = datetime.utcnow() - timedelta(hours=1)

    def _s3_scan(self):
        thour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        missing = collections.defaultdict(set)
        scanned = self._last_scan
        for jhr in xrange(-1,5):
            tstamp = thour + timedelta(hours=jhr)
            pback = os.path.join(S3_EVENTS_DIR, tstamp.strftime("%Y-%m-%d_%H.json"))
            kback = self._bucket.get_key(pback)
            if (kback is None): continue
            last_mod = datetime.strptime(kback.last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            if (last_mod > self._last_scan):
                fdata = kback.get_contents_as_string()
                scanned = datetime.utcnow()
                ff = json.loads(fdata)
                missing[tstamp].update((impid for impid in ff if ((impid not in self._obs_impid) and (impid not in self._missing))))
            kback.close()

        if (len(self._obs_impid)==0): return
        missingcount = sum((len(x) for x in missing.values()))
        obs_range = max(self._obs_impid.values()) - min(self._obs_impid.values())
        self._plog.info('Observed imps: #{0}  Time Span: {1:.0f}s   New Missing: {2}'.format(len(self._obs_impid), obs_range.total_seconds(), missingcount))
        tv = sorted(missing.keys())
        for tstamp in tv:
            if (len(missing[tstamp])>0):
                for imp in missing[tstamp]:
                    self._plog.warn("Missing {0} @ {1}".format(imp, tstamp.strftime("%Y-%m-%d_%H")))
                    self._missing[imp] = tstamp
        if (obs_range < timedelta(hours=4)): return
        #
        # alert goes here
        # 
        if (missingcount > 5):
            self._plog.warn("New missing count {0] exceeds threshold - alerting".format(missingcount))



    def _slow_imp_check(self):
        #
        #   check for missing impressions that were just late
        #
        for (impid, tstamp) in self._obs_impid.items():
            if (impid in self._missing):
                self._plog.warn("Missing impression {0] was just delayed".format(impid))
                self._wrlock.acquire()
                del self._missing[impid]
                self._wrlock.release()
            

    def _expunge_old_imp(self):
        del_keys = []
        tthres = datetime.utcnow() - timedelta(hours=6)
        for (impid, tstamp) in self._obs_impid.items():
            if (tstamp < tthres):
                del_keys.append(impid)
        if (len(del_keys)>0):
            self._plog.info('Expunging {0} old impressions'.format(len(del_keys)))
            self._wrlock.acquire()
            for impid in del_keys:
                del self._obs_impid[impid]
            self._wrlock.release()

        del_keys = []
        for (impid, tstamp) in self._missing.items():
            if (tstamp < tthres):
                del_keys.append(impid)
        if (len(del_keys)>0):
            self._plog.info('Expunging {0} old missing imp'.format(len(del_keys)))
            self._wrlock.acquire()
            for impid in del_keys:
                del self._missing[impid]
            self._wrlock.release()


    def periodic(self):
        self._slow_imp_check()
        self._s3_scan()
        self._expunge_old_imp()


    def process(self, vdata, **kwargs):
        self.start(**kwargs)
        for evt in vdata:
            self._evt_count += 1
            if ('impression_id' not in evt): continue
            v2 = evt['impression_id'].split("_")
            if (len(v2) != 2): continue
            lii = int(v2[1])
            if (lii == LINE_ITEM_ID):
                evtt = datetime.strptime(evt['timestamp'], "%Y-%m-%d %H:%M:%S")
                impid = evt["impression_id"]
                self._plog.debug("{0}   {1}".format(impid, ':'.join((x[0] for x in evt['event_list']))))
                self._wrlock.acquire()
                self._obs_impid[impid] = datetime.utcnow()
                self._wrlock.release()

traffic_checker = TrafficChecker()


def checktraffic(vdata, **kwargs):
    if (not isinstance(vdata, list)): return
    if (not isinstance(vdata[0], dict)): return
    if ('event_list' not in vdata[0]): return
    traffic_checker.process(vdata, **kwargs)


                
            
            


        
