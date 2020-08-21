from kinesisbolt import KinesisSpoutUnbundler
from bolt import BoltBase
from threading import Timer, Lock
from datetime import datetime
from affine.set_librato import SetLibrato
from affine import config
from streaming.util import IntervalDispatcher
import time
import boto
import collections
import sys
import copy


class IntervalStatsBoltBase(BoltBase):
    """ Base class used for compiling counters derived from event stream.

    This is an alternate means of collecting metric data and forwarding them to a reporting service
    -- the preferred architecture is currently defined in streaming/plugin.py and is used 
    in several services in affine/streaming/plugins.
    """
    _REPORTING_INTERVAL = 30
    #
    # _REPORTING_SUB_TOPIC         
    #   ** subclasses must define this to route metrics to a reporting service **
    #
    def __init__(self, *args, **kwargs):
        super(IntervalStatsBoltBase, self).__init__(*args, **kwargs)
        self._evt_grandtotal = 0
        self._counter_lock = Lock()
        self._counters = collections.defaultdict(int)
        self._tdispatcher = IntervalDispatcher(self._REPORTING_INTERVAL, self, 'pushstats')
        self._stats_client = SetLibrato(config, self._REPORTING_SUB_TOPIC)
        self._tdispatcher.dispatch()

    def pushstats(self):
        old_counters = {}
        self._counter_lock.acquire()
        for counter in self._counters:
            old_counters[counter] = self._counters[counter]
            self._counters[counter] = 0
        self._counter_lock.release()

        for counter in old_counters:
            self._stats_client.gauge(counter, old_counters[counter])

    def shutdown(self):
        self.cancelstats()

    def cancelstats(self):
        self._tdispatcher.cancel()



class AdServMetricsRaw(IntervalStatsBoltBase):
    '''
    Simple example, counts by event-type
    '''
    _REPORTING_INTERVAL = 300
    _REPORTING_SUB_TOPIC = "adsquery.queries."
    _PUBLISHED_COUNTERS = ('complete','query','view')
    def __init__(self, *args, **kwargs):
        super(AdServMetricsRaw, self).__init__(*args, **kwargs)
        self._aggcnt = 0

    def process(self, event):
        self._aggcnt += 1
        if ('impression_id' in event):
            self._counter_lock.acquire()
            self._counters['total'] += 1
            if (event.get('event_type') in self._PUBLISHED_COUNTERS):
                self._counters[event['event_type']] += 1
            self._counter_lock.release()
        return event



class AdServMetricsTrues(IntervalStatsBoltBase):
    '''
    Example, counts trues by line-item
    '''
    _REPORTING_INTERVAL = 300
    _REPORTING_SUB_TOPIC = "adsquery.metrics.trues."
    def __init__(self, *args, **kwargs):
        super(AdServMetricsTrues, self).__init__(*args, **kwargs)

    def process(self, event):
        if (event['event_type'] == 'query'):
            for (litem, response, reason, resp_reason) in event.get('line_item_responses', []):
                if (response == 'True'):
                    self._counter_lock.acquire()
                    self._counters[str(litem)] += 1
                    self._counters['total'] += 1
                    self._counter_lock.release()

        return event





if __name__ == "__main__":
    #
    #  cmd-line args
    #
    cmd_line_setters = {
        'V' : ('debug', None),
        's' : ('stream', (lambda x:x)),
        'i' : ('shardindex', int),
        'p' : ('poll', float),
        }
    opts = collections.defaultdict(bool)
    argv = sys.argv[1:]
    while (len(argv)):
        arg = argv.pop(0)
        if arg.startswith('-') and (arg[1] in cmd_line_setters.keys()):
            (label, transform) = cmd_line_setters[arg[1]]
            if (transform is not None):
                val = argv.pop(0)
                opts[label] = transform(val)
            else:
                opts[label] = True
        else:
            sys.stderr.write("Usage: %s [-s streamname] [-i shardindex]\n" % (__file__,))
            sys.exit(1)
    #
    #  Get stream info, we launch one spout per shard, so must launch
    #  as many of these MetricBolts as there are shards
    #
    source_stream = opts.get("stream", "AdsQueryRaw")
    skinconn = boto.connect_kinesis()
    strinfo = skinconn.describe_stream(source_stream)
    shv = strinfo['StreamDescription']['Shards']

    shimap = dict(((sidx, shv[sidx]['ShardId']) for sidx in xrange(len(shv))))
    shards = dict(((shardinfo['ShardId'],{}) for shardinfo in strinfo['StreamDescription']['Shards']))
    shardid = shimap[opts.get('shardindex', 0)]
    #
    #
    #  Just-in-time metrics, seek to the end of event stream
    #  Only care about live events
    #
    kinspout = KinesisSpoutUnbundler(source_stream, shardid, 
                                     start_at='LATEST',
                                     polling_interval=opts.get('poll',5.0))
    #
    #  Instantiate our bolts which specializes different metrics
    #  NB. each bolt is independent, so can add or remove w/o side-effects
    #
    amraw = AdServMetricsRaw()
    kinspout.addsink(amraw)
    amtrues = AdServMetricsTrues()
    kinspout.addsink(amtrues)
    #
    #  Now run the spout (i.e. start pulling events from the stream)
    #  In production this should be a daemon and should run indefinitely
    #
    try:
        kinspout.run()
    except KeyboardInterrupt:
        print 'Caught KEYBOARD INTR'
        kinspout.cancel()
        sys.exit(0)
