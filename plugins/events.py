import collections
import logging
import re
import time
from plugin import PeriodicPluginBase
from affine.set_librato import SetLibrato
from affine import config
import threading

plugins = ["eventsbydomain"]

domain_regex = re.compile(r'^(https?\:)?(\/\/)?([\w\-\.\_]*\.)?([\w+\-\_]+\.\w{2,5})(\/[\S ]*)?$')


def url2domain(uri):
    mo = domain_regex.match(uri)
    return (mo) and mo.group(4) or None


class EventsByDomainPlugin(PeriodicPluginBase):
    '''
    Plugin counts queries and impressions by; (a) all domains (b) only youtube.com
    and pushes counters to librato
    '''
    _REPORTING_SUB_TOPIC2 = "adsquery.totals."

    def __init__(self, *args, **kwargs):
        super(EventsByDomainPlugin, self).__init__(300, *args, **kwargs)
        self._evt_count = 0
        env = config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._stats_client2 = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC2)
        self._wrlock = threading.Lock()
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._plog.info("init called")
        self._allctr = {}
        self._evttypes = ('query', 'view', 'click', 'complete')
        self._reset_counters()

    def periodic(self):
        tnow = int(time.time())
        gauge_opts = dict(measure_time=tnow)
        if ('librato_source' in self._opts):
            gauge_opts['source'] = self._opts['librato_source']

        librmetrics = {}
        for ctr in self._allctr:
            librmetrics[ctr] = dict(gauge=self._allctr[ctr])
            librmetrics[ctr].update(gauge_opts)
        librmetrics['activelineitems'] = dict(gauge=len(self._activelineitems))
        librmetrics['activelineitems'].update(gauge_opts)
        self._stats_client2.bulk_gauge(librmetrics)
        self._plog.info('Librato updated {0} total counters'.format(len(librmetrics)))
        self._reset_counters()

    def _reset_counters(self):
        self._wrlock.acquire()
        self._allctr = collections.Counter()
        self._activelineitems = set()
        self._wrlock.release()

    def process(self, vdata, **kwargs):
        media_partner = 'Cobalt'
        self.start(**kwargs)
        for data in vdata:
            if "events" in data:
                self._wrlock.acquire()
                events = data['events']
                for event in events:
                    self._evt_count += 1
                    if 'no_match' in events:
                        self._allctr[media_partner + ".False"] += len(events['no_match'])
                    elif 'match' in events:
                        self._allctr[media_partner + ".True"] += len(events['match'])
                    else:
                        if event in self._evttypes:
                            self._allctr[media_partner + "." + event] += 1
                    for li in events[event]:
                        self._activelineitems.add(li)
                self._wrlock.release()

eventsbydomain_plugin = EventsByDomainPlugin()


def eventsbydomain(vdata, **kwargs):
    if (not isinstance(vdata, list)):
        return
    eventsbydomain_plugin.process(vdata, **kwargs)
