import json
import re
import sys
from collections import defaultdict, deque, Counter
import Queue
import gzip
import logging
import threading
import time
import os
from streaming.util import IntervalDispatcher, shardid2index, snaphour
from datetime import datetime, timedelta
from bolt import BoltBase
from affine.model import LineItem, Campaign
from streaming.util import datamodel
from affine.log import configure_logging

roll_log = logging.getLogger('lineitemrollup')

class LineItemRollup(BoltBase):
    _VIEWABILITY_LABEL_ID = 7840
    _MYSQL_NULL = "\\N"
    _PROCESSOR_NAME = "adslineitemrollup"
    def __init__(self, shardid, *args, **kwargs):
        super(LineItemRollup, self).__init__(*args, **kwargs)
        self._tdumper = IntervalDispatcher(3600, self, 'dumper', daemon=True)
        self._shardid = shardid
        self._shardindex = shardid2index(shardid)
        self._event_count = 0
        self._imp_count = 0
        self._log = logging.getLogger("__main__")
        self._log.setLevel(logging.INFO)
        outdir = LineItemRollup.output_dir()
        if (not os.path.exists(outdir)):
            self._log.info("Creating output directory {0}".format(outdir))
            os.makedirs(outdir)
        self._lic = {}
        self._brand_unsafe_labels = {}
        self._lineitem = {}
        self._campaign = {}
        self._cntrmap = {
            'query':0,
            'view':1,
            'firstQuartile':2,
            'midpoint':3, 
            'thirdQuartile':4, 
            'complete':5, 
            'click':6, 
            'skip':7,
            'viewableimp':8,
            'brandsafe':9,
            'trues':10,
            'falses':11,
            'nones':12,
            'knownimp':13
            }
        self._resmap = {
            "True" : "trues",
            "False" : "falses",
            "None" : "nones"
            }
        self._started = False
        self._wrlock = threading.Lock()
        self._dumplock = threading.Lock()

    @classmethod
    def output_dir(cls):
        return os.path.join(os.getenv("HOME"), "output", LineItemRollup._PROCESSOR_NAME)

    @classmethod
    def output_prefix(cls, tnow):
        fpref = "lineitemru.{0}".format(tnow.strftime("%Y-%m-%d-%H-%M-%S"))
        return os.path.join(LineItemRollup.output_dir(), fpref)

    def _start(self):
        if (not self._started):
            self._tdumper.dispatch()
            self._started = True

    def dumper(self):
        self._wrlock.acquire()
        lictr = self._lic
        self._lic = {}
        self._wrlock.release()
        self._dumplock.acquire()
        tnow = datetime.utcnow() + timedelta(minutes=5) - timedelta(hours=1)
        tnow = snaphour(now=tnow)
        tdbstr = tnow.strftime("%Y-%m-%d %H:%M:%S")
        fprefix = LineItemRollup.output_prefix(tnow)
        opath = fprefix + ".{0:03d}.tsv".format(self._shardindex)
        self._log.info("Dumping {0} line items to {1}".format(len(lictr), opath))
        with open(opath, "w") as fout:
            for (liid,counters) in lictr.items():
                lineitem = LineItem.get(liid)
                if (lineitem):
                    pubid = lineitem.publisher_id or self._MYSQL_NULL
                    campid = lineitem.campaign_id
                    campaign = Campaign.get(campid)
                    if (campaign):
                        fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(tdbstr, 
                                                                           liid, 
                                                                           campid,
                                                                           pubid,
                                                                           campaign.advertiser_id,
                                                                           '\t'.join(map(str, counters))))
        self._dumplock.release()
        
    def _evt2incr(self, elist):
        incr = [0]*len(self._cntrmap)
        for (etype,ts) in elist:
            if ((etype in self._cntrmap) and (etype != 'query')):
                incr[self._cntrmap[etype]] = 1
        return incr
    
    def process(self, imp):
        self._start()
        self._event_count += 1
        if ('imp_line_item_id' in imp):
            self._imp_count += 1
            liid = int(imp['imp_line_item_id'])
            elist = imp['event_list']
            has_view_event = 'view' in map((lambda x:x[0]), elist)
            incr = self._evt2incr(elist)
            if (liid not in self._brand_unsafe_labels):
                self._brand_unsafe_labels[liid] = set(datamodel.get_brand_unsafe_label_ids([liid]))
            self._wrlock.acquire()
            if (liid not in self._lic):
                self._lic[liid] = [0]*len(self._cntrmap)
            self._lic[liid] = map(sum, zip(self._lic[liid], incr))
            if (has_view_event) and ('true_label_ids' in imp):
                self._lic[liid][self._cntrmap['viewableimp']] += (0,1)[(self._VIEWABILITY_LABEL_ID in imp['true_label_ids'])]
                self._lic[liid][self._cntrmap['brandsafe']] += (0,1)[not self._brand_unsafe_labels[liid] & set(imp['true_label_ids'])]
                self._lic[liid][self._cntrmap['knownimp']] += 1
            self._wrlock.release()

        lir = imp.get('line_item_responses', [])
        for (liid, result, dum1, dum2) in lir:
            self._wrlock.acquire()
            if (liid not in self._lic):
                self._lic[liid] = [0]*len(self._cntrmap)
            if (result in self._resmap):
                self._lic[liid][self._cntrmap[self._resmap[result]]] += 1
            self._lic[liid][self._cntrmap['query']] += 1
            self._wrlock.release()
        return (False,None)

    
if __name__ == "__main__":
    import sys
    import boto
    import time
    import subprocess
    import os
    import signal
    import collections
    import affine.config
    from kinesisbolt import KinesisSpoutUnbundler, KinesisSink
    import daemon
    from daemon.pidfile import TimeoutPIDLockFile

    shards = {}

    def handleTERM(signum, frame):
        xlog = logging.getLogger("__main__")
        xlog.warn('Got term.  killing children')
        for sh in shards.values():
            sh['proc'].terminate()
        for sh in shards.values():
            rval = sh['proc'].wait()
            sh['retval'] = retval
        xlog.warn('All children terminated... exiting')
        sys.exit(0)
            
    def main_program(opts):
        signal.signal(signal.SIGTERM, handleTERM)
        configure_logging("adslineitemrollup.log")
        xlog = logging.getLogger("__main__")
        skinconn = boto.connect_kinesis()
        strinfo = skinconn.describe_stream(opts['source_stream'])
        skinconn.close()
        ss = dict(((shardinfo['ShardId'],{}) for shardinfo in strinfo['StreamDescription']['Shards']))
        for (k,v) in ss.items():
            shards[k] = v
        sidx = 1
        xlog.warn('Spawning {0} child procs'.format(len(shards)))
        for (shardid,sh) in shards.items():
            childargs = filter((lambda x: x!='-s'), sys.argv[1:])
            childargs.extend(['-S', shardid])
            xlog.warn('Shard ID %s' % affine.config.log_dir())
            logpath = os.path.join(affine.config.log_dir(), "adslineitemrollup_{0}.log".format(sidx))
            logpath = os.path.normpath(logpath)
            childargs.extend(['-l', logpath])
            xlog.warn('Invoking child with args: %s' % str(childargs))
            p = subprocess.Popen([sys.executable, __file__] + childargs, stdin=subprocess.PIPE)
            sh['proc'] = p
            sh['retval'] = None
            sidx += 1
            time.sleep(1.0)

        isdone = False
        while (not isdone):
            for sh in shards.values():
                if (sh['retval'] is None):
                    retval = sh['proc'].poll()
                    if (retval is not None):
                        sh['retval'] = retval
                        xlog.warn('[%d] child terminated (%s)' % (sh['proc'].pid, retval))

            time.sleep(1.0)
            isdone = all((x['retval'] is not None) for x in  shards.values())
        xlog.warn('All child process exited... exiting')
            
    def runner(shardid, **kwargs):
        xlog = logging.getLogger("__main__")
        xlog.warn('Runner started with shard = %s' % shardid)
        lineitemrollup = LineItemRollup(shardid, **opts)
        kinspout = KinesisSpoutUnbundler(opts['source_stream'], shardid)
        kinspout.addsink(lineitemrollup)
        try:
            kinspout.run()
        except KeyboardInterrupt:
            kinspout.shutdown()
            raise

            
    opts = collections.defaultdict(bool)
    argv = sys.argv[1:]
    cmd_line_setters = {
        's' : ('spawn', None),
        'S' : ('shardid', (lambda x: x)),
        'V' : ('debug', None),
        'I' : ('interactive', None),
        'l' : ('logpath', (lambda x:x)),
        }
    streamnames = []
    while (len(argv)):
        arg = argv.pop(0)
        if arg.startswith('-'):
            if (arg[1] in cmd_line_setters.keys()):
                (label, transform) = cmd_line_setters[arg[1]]
                if (transform is not None):
                    val = argv.pop(0)
                    opts[label] = transform(val)
                else:
                    opts[label] = True
            else:
                sys.stderr.write("Unrecognized option: " + arg + "\n")
                sys.exit(1)
        else:
            streamnames.append(arg)

    if (len(streamnames)!=1):
        sys.stderr.write("Usage: adslineitemrollup.py [opts] source_stream\n")
        sys.exit(1)
    opts['source_stream'] = streamnames[0]
    skinconn = boto.connect_kinesis()
    try:
        strinfo = skinconn.describe_stream(opts['source_stream'])
    except:
        sys.stderr.write('Failed to retrieve stream [{0}]. Aborting'.format(opts['source_stream']))
        sys.exit(1)
    skinconn.close()

    if (opts['spawn']):
        if (not opts['interactive']):
            # daemon
            working_dir = os.path.dirname(os.path.realpath(__file__))
            dcontext = daemon.DaemonContext(working_directory=working_dir,
                                            pidfile=TimeoutPIDLockFile('/tmp/adslineitemrollup.pid'))
            with dcontext:
                main_program(opts)

        else:
            # interactive
            logging.basicConfig()
            xlog = logging.getLogger("__main__")
            xlog.setLevel(logging.INFO)
            xlog.info('Options: {0}'.format(opts))
            xlog.info('Source stream: {0}   Sink stream: {1}'.format(opts['source_stream'], opts['sink_stream']))
            shards = dict(((shardinfo['ShardId'],{}) for shardinfo in strinfo['StreamDescription']['Shards']))
            for (shardid,sh) in shards.items():
                childargs = filter((lambda x: x!='-s'), sys.argv[1:])
                childargs.extend(['-S', shardid])
                xlog.warn('Invoking child with args: %s' % str(childargs))
                p = subprocess.Popen([sys.executable, __file__] + childargs, stdin=subprocess.PIPE)
                sh['proc'] = p
                time.sleep(1.0)

            try:
                dead_pool = []
                while (len(dead_pool)<len(shards)):
                    time.sleep(0.1)
                    for (shardid,sh) in shards.items():
                        if ('retval' not in sh):
                            retval = sh['proc'].poll()
                            if (retval is not None):
                                sh['retval'] = retval
                                xlog.warn('[%d] child terminated (%s)' % (sh['proc'].pid, retval))
                                dead_pool.append(sh)
            except KeyboardInterrupt:
                print 'Keyboard Interrupt (parent)'

    elif (opts['shardid']):
        if (opts['interactive']):
            logging.basicConfig()
        else:
            configure_logging(opts["logpath"])
        xlog = logging.getLogger("__main__")
        xlog.setLevel(opts['debug'] and logging.DEBUG or logging.INFO)
        shardid = opts['shardid']
        del opts['shardid']
        xlog.info('Child process {0} assigned shard ID: {1}  Starting Seqnum: {2}'.format(os.getpid(), shardid, opts.get('last_seqnum',None)))
        try:
            runner(shardid, **opts)
        except KeyboardInterrupt:
            print 'Keyboard Interrupt child:', os.getpid()
            
        
    

