import collections
import logging
import time
import calendar
from datetime import datetime
from streaming.plugin import PeriodicPluginBase
from affine.set_librato import SetLibrato
from affine import config
import threading

plugins = [ "trafficmonitor_process" ]

class TrafficMonitor(PeriodicPluginBase):
    _REPORTING_SUB_TOPIC = "streaming.trafficmonitor."
    def __init__(self, *args, **kwargs):
        super(TrafficMonitor, self).__init__(600, *args, **kwargs)
        env = config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._stats_client = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC)
        self._wrlock = threading.Lock()
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._evttypes = set(['view', 'complete', 'click'])
        self._lineitem_map = {
            7000: 'sj2',
            7001: 'dc6',
            7002: 'ams5'
        }
        self._lineitem_watched = tuple(self._lineitem_map)
        self._evt_count = 0
        self._ewin = collections.deque()
        self._last_seen = {}
        
    def _wintrim(self):
        tnow = int(time.time())
        while ((len(self._ewin)) and ((tnow - self._ewin[0][1]) > 4200)):
            self._ewin.popleft()

    def periodic(self):
        fnow = datetime.utcnow()
        fnow = datetime(fnow.year, fnow.month, fnow.day, fnow.hour, (fnow.minute//10)*10)
        tnow = calendar.timegm(fnow.timetuple())
        gauge_opts = dict(measure_time=tnow)
        if ('librato_source' in self._opts):
            gauge_opts['source'] = self._opts['librato_source']
        self._wrlock.acquire()
        self._wintrim()
        cntr = collections.Counter()
        for (label, ts) in self._ewin:
            cntr[label] += 1
        self._wrlock.release()

        lmetrics = {}
        for label in cntr:
            clabel = 'count.'+label
            lmetrics[clabel] = dict(gauge=cntr[label])
            lmetrics[clabel].update(gauge_opts)
        self._stats_client.bulk_gauge(lmetrics)
        self._plog.info('Gauge Opts: {0}   #Gauges {1}'.format(gauge_opts, dict(cntr)))

        lvmetrics = []
        for label in self._last_seen.keys():
            clabel = 'last_seen.'+label
            for shardid in self._last_seen[label].keys():
                lvmetrics.append(dict(name=clabel, value=(tnow - self._last_seen[label][shardid]),
                                      source=str(shardid), measure_time=tnow))
        self._stats_client.bulk_gauge(lvmetrics)
        self._plog.info('Last Seen   #Gauges {0}'.format(lvmetrics))


    def process(self, vdata, **kwargs):
        self.start(**kwargs)
        for data in vdata:
            self._evt_count += 1
            try:
                liid = int(data.get('imp_line_item_id',0))
            except:
                self._plog.warn("Not a valid line item id - {0}".format(data.get('imp_line_item_id',0)))
                liid = None
            if (liid) and (liid in self._lineitem_watched):
                label = self._lineitem_map[liid]
                if (label not in self._last_seen):
                    self._last_seen[label] = {}
                iid = data.get('impression_id', '_')
                evts = set((x[0] for x in data['event_list']))
                uevts = evts & self._evttypes
                if (uevts):
                    tnow = time.time()
                    eshardid = None
                    for e in evts:
                        if (e.startswith('tracker')):
                            eshardid = int(e.split("_")[1])
                    self._wrlock.acquire()
                    if (eshardid is not None):
                        lastt = self._last_seen[label].get(eshardid, 0)
                        self._last_seen[label][eshardid] = tnow
                    self._wintrim()
                    self._ewin.append((label, tnow))
                    self._wrlock.release()

            
        
trafficmonitor = TrafficMonitor()


def trafficmonitor_process(vdata, **kwargs):
    if (not isinstance(vdata, list)): return
    if ('event_list' not in vdata[0]): return
    trafficmonitor.process(vdata, **kwargs)
