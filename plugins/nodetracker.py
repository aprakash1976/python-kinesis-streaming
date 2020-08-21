import logging
import time
from plugin import PeriodicPluginBase
from affine.set_librato import SetLibrato
from affine import config
import threading

#
#  when the module is imported on startup, this
#  array specifies which plugin (function/callback)
#  to invoke
#
plugins = ["node_tracker"]


class NodeTrackerPlugin(PeriodicPluginBase):
    '''
    Plugin counts requests per server instance
    and pushes counters to librato
    '''
    _REPORTING_SUB_TOPIC = "adsquery.nodetracker."

    def __init__(self, *args, **kwargs):
        super(NodeTrackerPlugin, self).__init__(300, *args, **kwargs)
        env = config.get('env')
        prefix = (env != 'production') and (env + '.') or ''
        self._stats_client = SetLibrato(config, prefix + self._REPORTING_SUB_TOPIC)
        self._wrlock = threading.Lock()
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._plog.info("init called")
        self._ctr = {}

    def periodic(self):
        tnow = int(time.time())
        lvmetrics = []
        for counter_name in self._ctr:
            lvmetrics.append(dict(name='last_seen', value=(tnow - self._ctr[counter_name]),
                                  source=str(counter_name), measure_time=tnow))
        self._stats_client.bulk_gauge(lvmetrics)
        self._plog.info('Librato updated {0} counters'.format(len(lvmetrics)))
        self._reset_counters()

    def _reset_counters(self):
        self._wrlock.acquire()
        # TODO: remove old nodes keys that are obsolete (how?)
        self._wrlock.release()

    def process(self, vdata, **kwargs):
        self.start(**kwargs)
        for data in vdata:
            self._wrlock.acquire()
            if "node_name" in data:
                self._ctr[data["node_name"]] = int(time.time())
            self._wrlock.release()

nodetracker = NodeTrackerPlugin()


def node_tracker(vdata, **kwargs):
    if (not isinstance(vdata, list)):
        return
    nodetracker.process(vdata, **kwargs)
