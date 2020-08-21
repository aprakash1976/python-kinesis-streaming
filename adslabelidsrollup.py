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
import signal
from streaming.util import IntervalDispatcher, shardid2index, snaphour
from streaming.kinesisbolt import KinesisSpoutUnbundler
from datetime import datetime, timedelta
from bolt import BoltBase
from affine.model import LineItem, Campaign
from affine.log import configure_logging
from affine.model import session, WebPageLabelResult, Label
from affine.cache import cache_get, cache_get_time, cache_set
from mpkinesismgr import MPKinesisMgr
from streaming.util.mprocmgr import MultiProcMgrBase
from streaming.util import args_parser

args_cfg = {
            "idx" : {
                    "tovalue" : int
                    },
          }

class LabelsRollup(BoltBase):
    _VIEWABILITY_LABEL_ID = 7840
    _MYSQL_NULL = "\\N"
    _PROCESSOR_NAME = "adslabelsrollup"
    _IAB_LABELS = (1041 ,1051 ,2136 ,2161 ,2184 ,2215 ,2325 ,2234 ,1055 ,2422 ,2441 ,2452 ,2459 ,2520 ,2758 ,2774 ,2765 ,2484 ,1057 ,2619 ,2632 ,2707)
    _LINE_ITEMS = dict()
    _PERS_LABELS = set()

    def __init__(self, *args, **kwargs):
	super(LabelsRollup, self).__init__(*args, **kwargs)
        self._event_count = 0
        self._imp_count = 0
        self._log = kwargs.get("logger", logging.getLogger("__main__"))
        self._shardid = kwargs.get("shard_id")
	self._shardindex = shardid2index(self._shardid)
        self._trueroll = {}
        if ("logger" not in kwargs):
            self._log.setLevel(logging.INFO)
        self._tdumper = IntervalDispatcher(3600, self, 'dumper', daemon=True)
        self._imp_count = 0
        self._event_count = 0
        self._imp_count = 0
        self._log = logging.getLogger("__main__")
        self._log.setLevel(logging.INFO)
        print '**********'
        print 'output dir'
        print LabelsRollup.output_dir()
        print '**********'
        outdir = LabelsRollup.output_dir()
        if (not os.path.exists(outdir)):
            self._log.info("Creating output directory {0}".format(outdir))
            os.makedirs(outdir)
        self._lic = {}
        self._brand_unsafe_labels = {}
        self._lineitem = {}
        self._campaign = {}
        self._cntrmap = {
            'view':0,
            'click':1,
            'firstQuartile':2,
            'midpoint':3, 
            'thirdQuartile':4, 
            'complete':5, 
            'skip':6
            }
        self._started = False
        self._wrlock = threading.Lock()
        self._dumplock = threading.Lock()
        self.load_line_items()
        self.load_personality_labels()

    @classmethod
    def output_dir(cls):
        return os.path.join(os.getenv("HOME"), "output", LabelsRollup._PROCESSOR_NAME)

    @classmethod
    def output_prefix(cls, tnow):
        fpref = "labelru.{0}".format(tnow.strftime("%Y-%m-%d-%H-%M-%S"))
        return os.path.join(LabelsRollup.output_dir(), fpref)

    def _start(self):
        if (not self._started):
            self._tdumper.dispatch()
            self._started = True

    def load_personality_labels(self):
        query = session.query(Label.id).filter_by(label_type='personality')
        for line_item_id in query:
            self._PERS_LABELS.update(line_item_id)

    def load_line_items(self):
        query = session.query(LineItem).filter_by(archived=0)
        for line_item in query:
            self._LINE_ITEMS[line_item.id] = line_item

    def dumper(self):
        print '***************************************'
        print 'calling dumper'
        print '***************************************'
        self._wrlock.acquire()
        lictr = self._lic
        self._lic = {}
        self._wrlock.release()
        self._dumplock.acquire()
        tnow = datetime.utcnow() + timedelta(minutes=5) - timedelta(hours=1)
        tnow = snaphour(now=tnow)
        tdbstr = tnow.strftime("%Y-%m-%d %H:%M:%S")
        fprefix = LabelsRollup.output_prefix(tnow)
        opath = fprefix + ".{0:03d}.tsv".format(self._shardindex)
        self._log.info("Dumping {0} line items to {1}".format(len(lictr), opath))
        with open(opath, "w") as fout:
            for key in lictr.iterkeys():
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t\n".format(key,
                                                                                    lictr[key]['advertiser_id'],
                                                                                    lictr[key]['camp_id'],
                                                                                    lictr[key]['pub_id'],
                                                                                    lictr[key]['line_item_id'],
                                                                                    tdbstr, 
                                                                                    lictr[key]['is_iab'],
                                                                                    lictr[key]['is_personality'],
                                                                                    '\t'.join(map(str, lictr[key]['counter']))))
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
            liid = int(imp['imp_line_item_id'])
            labelids = imp['true_label_ids'] 
            incr = self._evt2incr(imp['event_list'])
            page_id = imp.get('page_id')
            if not (page_id is None):
                true_label_ids = self.get_true_label_ids(page_id)
		'''
                print true_label_ids
                print labelids
		'''
                real_label_ids = list(set(labelids) & true_label_ids)
                self._wrlock.acquire()
                for label_id in real_label_ids:
                    is_iab = label_id in self._IAB_LABELS
                    is_personality = label_id in self._PERS_LABELS
                    if liid in self._LINE_ITEMS:
                        if label_id not in self._lic:
                            self._lic[label_id] = {}
                            self._lic[label_id]['counter'] = [0]*len(self._cntrmap)
                        lineitem = self._LINE_ITEMS[liid]
                        if not lineitem:
                            self.load_line_items()
                            lineitem = self._LINE_ITEMS[liid]
                        if lineitem:
                            pubid = lineitem.publisher_id or self._MYSQL_NULL
                            campid = lineitem.campaign_id
                            campaign = Campaign.get(campid)
                            if (campaign):
                                self._lic[label_id]['advertiser_id'] = int(campaign.advertiser_id)
                                self._lic[label_id]['camp_id'] = int(campid)
                                self._lic[label_id]['pub_id'] = pubid
                            else:
                                self._lic[label_id]['advertiser_id'] = 0
                                self._lic[label_id]['camp_id'] = 0
                                self._lic[label_id]['pub_id'] = 0
                        self._lic[label_id]['line_item_id'] = int(liid)
                        self._lic[label_id]['is_iab'] = is_iab
                        self._lic[label_id]['is_personality'] = is_personality
                        self._lic[label_id]['counter'] = (map(sum, zip(self._lic[label_id]['counter'], incr)))
                self._wrlock.release()
	    '''
            print self._lic
	    '''
        return (False,None)

    def get_true_label_ids(self, page_id):
        label_ids = cache_get(str(page_id))
        print 'calling memcache'
        print label_ids
        if not label_ids:
            label_ids = set()
            results = WebPageLabelResult.query.filter_by(page_id=page_id).all()
            for result in results:
                '''
                label_ids[int(result.label.id)] = True if result.label.label_type == 'personality' else False
                cache_set(str(page_id), label_ids)
                '''
                label_ids.add(result.label.id)
            cache_set(str(page_id), label_ids)
        else:
            if type(label_ids) == type(dict()):
                print 'converting to set'
                return set(label_ids.keys())
        return label_ids
    
class LabelMgr(MPKinesisMgr):

    def __init__(self, processname, *args, **kwargs):
        super(LabelMgr, self).__init__(processname, *args, **kwargs)

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
        self._worker = LabelsRollup(logger=self._log, shard_id=self._shard['ShardId'])
        self._spout.addsink(self._worker)
        self._spout.run()
        self._log.info("Main exit")
        return None

if __name__ == "__main__":
    t = LabelMgr("labelroll_infobright")
    t.start()
    sys.exit(0) 
        
    

