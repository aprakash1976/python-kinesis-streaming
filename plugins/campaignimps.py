import collections
import logging
import re
import time
from datetime import datetime
from streaming.plugin import PeriodicPluginBase
from affine.set_librato import SetLibrato
from affine import config
from affine.model import LineItem, session
from affine.retries import retry_operation
import threading

plugins = [ "campaign_imps" ]

domain_regex = re.compile(r'^(https?\:)?(\/\/)?([\w\-\.\_]*\.)?([\w+\-\_]+\.\w{2,5})(\/[\S ]*)?$')

def url2domain(uri):
    mo = domain_regex.match(uri)
    return (mo) and mo.group(4) or None


def lineitem2campaign(line_item_id):
    li = retry_operation(LineItem.get, [int(line_item_id)], sleep_time=5)
    campaign = None
    if (li):
        campaign = li.campaign
    session.close()
    return campaign


class CampaignImpsPlugin(PeriodicPluginBase):
    _REPORTING_SUB_TOPIC = "adsquery.totals."
    _REPORTING_SUB_TOPIC2 = "adsquery.lineitem."
    _REPORTING_SUB_TOPIC3 = "adsquery.camp."
    def __init__(self, *args, **kwargs):
        super(CampaignImpsPlugin, self).__init__(600, *args, **kwargs)
        self._campname = {}
        self._li2camp = {}
        self._li_cntr  = {}
        self._evt_count = 0
        env = config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._stats_client = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC)
        self._stats_client2 = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC2)
        self._stats_client3 = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC3)
        self._wrlock = threading.Lock()
        self._count_threshold = kwargs.get('count_threshold', 10)
        self._stripspc = re.compile(r'\s+')
        self._stripnonword = re.compile(r'\W+')
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._evttypes = set(('view', 'click', 'complete'))
        self._querycntrs = ("query","trues","falses")

        
    def _str2label(self, name):
        return self._stripnonword.sub('', self._stripspc.sub('_', name))


    def periodic(self):
        self._wrlock.acquire()
        lin_cntr = self._li_cntr
        self._li_cntr  = {}
        self._wrlock.release()
        tnow = int(time.time())
        gauge_opts = dict(measure_time=tnow)
        if ('librato_source' in self._opts):
            gauge_opts['source'] = self._opts['librato_source']
        self._plog.info('Gauge Opts: {0}'.format(gauge_opts))

        camp_ctr = {}
        librmetrics = {}
        librmetrics['activelineitems'] = dict(gauge=len(lin_cntr))
        librmetrics['activelineitems'].update(gauge_opts)
        self._stats_client.bulk_gauge(librmetrics)
        librmetrics = {}
        report_evttypes = tuple(self._evttypes) + self._querycntrs
        for liid,c in lin_cntr.items():
            self._plog.debug("{0}  {1}".format(liid, ','.join((e+':'+str(c[e]) for e in report_evttypes))))
            cpid = self._li2camp.get(liid)
            if (cpid):
                if (cpid not in camp_ctr):
                    camp_ctr[cpid] = self._new_counters()
            else:
                self._plog.info("{0}  missing campaign id".format(liid))

            for evt in report_evttypes:
                gauge = str(liid) + '.' + evt 
                librmetrics[gauge] = dict(gauge=c[evt])
                librmetrics[gauge].update(gauge_opts)
                if (cpid is not None):
                    camp_ctr[cpid][evt] += c[evt]

        ngauges = len(librmetrics)
        self._stats_client2.bulk_gauge(librmetrics)
        del lin_cntr

        librmetrics = {}
        for cpid,c in camp_ctr.items():
            if (cpid not in self._campname):
                self._plog.warn("Cannot find campaign name for {0}".format(cpid))
                continue

            cname = self._campname[cpid]
            for evt in report_evttypes:
                gauge = cname + '.' + evt 
                librmetrics[gauge] = dict(gauge=c[evt])
                librmetrics[gauge].update(gauge_opts)
                self._plog.debug('Librato gauges: {0} = {1}'.format(gauge, c[evt]))
        self._stats_client3.bulk_gauge(librmetrics)
        ngauges += len(librmetrics)
        self._plog.info('Librato updated  #gauges: {0}'.format(ngauges))

    def _new_counters(self):
        return dict(((e,0) for e in self._evttypes|set(self._querycntrs)))

    def process(self, vdata, **kwargs):
        self.start(**kwargs)
        for data in vdata:
            self._evt_count += 1
            evts = set((x[0] for x in data['event_list']))
            uevts = evts & self._evttypes
            imp_line_item_id = None

            if (uevts) and data.get('imp_line_item_id'):
                self._plog.debug("Evts: {0}".format(','.join(uevts)))
                imp_line_item_id = int(data['imp_line_item_id'])
                self._wrlock.acquire()
                if (imp_line_item_id not in self._li_cntr):
                    self._li_cntr[imp_line_item_id] =  self._new_counters()

                for e in uevts:
                    self._li_cntr[imp_line_item_id][e] += 1
                self._wrlock.release()
            
            # collect queries per line item
            lir = data.get('line_item_responses', [])
            for (liid, result, dum1, dum2) in lir:
                self._wrlock.acquire()
                if (liid not in self._li_cntr):
                    self._li_cntr[liid] =  self._new_counters()
                self._li_cntr[liid]['query'] += 1
                self._li_cntr[liid]['trues'] += (0,1)[result == 'True']
                self._li_cntr[liid]['falses'] += (0,1)[result == 'False']
                self._wrlock.release()
                if (liid not in self._li2camp):
                    # lookup DB for unknown line items
                    campaign = lineitem2campaign(liid)
                    if (campaign is not None):
                        self._plog.info('[{0}] mapped -> [{1}]<{2}>'.format(liid, campaign.id, campaign.name))
                        cpid = int(campaign.id)
                        self._wrlock.acquire()
                        self._campname[cpid] = self._str2label(campaign.name)
                        self._li2camp[liid] = cpid
                        self._wrlock.release()
                    else:
                        self._wrlock.acquire()
                        self._li2camp[liid] = None
                        self._wrlock.release()

        


campimps = CampaignImpsPlugin()


def campaign_imps(vdata, **kwargs):
    if (not isinstance(vdata, list)): return
    if ('event_list' not in vdata[0]): return
    campimps.process(vdata, **kwargs)
