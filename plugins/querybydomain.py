import collections
import logging
import re
import time
from plugin import PeriodicPluginBase
from affine.set_librato import SetLibrato
from affine import config
import threading

plugins = ["querybydomain"]

domain_regex = re.compile(r'^(https?\:)?(\/\/)?([\w\-\.\_]*\.)?([\w+\-\_]+\.\w{2,5})(\/[\S ]*)?$')


def url2domain(uri):
    mo = domain_regex.match(uri)
    return (mo) and mo.group(4) or None


class QueryByDomainPlugin(PeriodicPluginBase):
    '''
    Plugin counts queries and impressions by; (a) all domains (b) only youtube.com
    and pushes counters to librato
    '''
    _REPORTING_SUB_TOPIC = "adsquery.bydomain."
    _REPORTING_SUB_TOPIC2 = "adsquery.totals."

    def __init__(self, *args, **kwargs):
        super(QueryByDomainPlugin, self).__init__(300, *args, **kwargs)
        self._evt_count = 0
        env = config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._stats_client = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC)
        self._stats_client2 = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC2)
        self._wrlock = threading.Lock()
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._plog.info("init called")
        self._ctr_names = ["query.all", "query.youtube"]
        self._ctr = {}
        self._allctr = {}
        self._reset_counters()

    def periodic(self):
        tnow = int(time.time())
        gauge_opts = dict(measure_time=tnow)
        if ('librato_source' in self._opts):
            gauge_opts['source'] = self._opts['librato_source']

        librmetrics = {}
        for counter_name in self._ctr_names:
            librmetrics[counter_name] = dict(gauge=self._ctr[counter_name])
            librmetrics[counter_name].update(gauge_opts)
        self._stats_client.bulk_gauge(librmetrics)
        self._plog.info('Librato updated {0} counters'.format(len(librmetrics)))
        librmetrics = {}
        for ctr in self._allctr:
            librmetrics[ctr] = dict(gauge=self._allctr[ctr])
            librmetrics[ctr].update(gauge_opts)
        self._stats_client2.bulk_gauge(librmetrics)
        self._plog.info('Librato updated {0} total counters'.format(len(librmetrics)))
        self._reset_counters()

    def _reset_counters(self):
        self._wrlock.acquire()
        for counter_name in self._ctr_names:
            self._ctr[counter_name] = 0
        self._allctr = collections.Counter()
        self._wrlock.release()

    def process(self, vdata, **kwargs):
        self.start(**kwargs)
        for data in vdata:
            if data.get("url") is not None:
                self._evt_count += 1
                domain = url2domain(data['url'])
                self._wrlock.acquire()
                self._ctr["query.all"] += 1
                if(domain == "youtube.com"):
                    self._ctr["query.youtube"] += 1
                self._allctr['bid_requests.all'] += 1
                if (data['content_id'] is not None):
                    self._allctr['bid_requests.known'] += 1
                self._wrlock.release()

querybydomain_plugin = QueryByDomainPlugin()


def querybydomain(vdata, **kwargs):
    if (not isinstance(vdata, list)):
        return
    querybydomain_plugin.process(vdata, **kwargs)
