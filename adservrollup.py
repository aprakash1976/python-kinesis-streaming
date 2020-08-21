import json
import re
import sys
import hashlib
import collections
import gzip
import logging
from datetime import datetime, timedelta
from bolt import BoltBase

roll_log = logging.getLogger('adservrollup')

class VideoUserLineItem(object):
    null_value = r'\\n'
    counter_label = (     # order in IB schema
        'query',
        'view',
        'click',
        'firstQuartile',
        'midpoint',
        'thirdQuartile',
        'complete',
        )
    def __init__(self, imp):
        self._video_id     = imp['video_id']
        self._user_id      = imp['user_id']
        self._line_item_id = imp['line_item_id']
        self._page         = imp['page']
        self._ctr          = collections.defaultdict(int)
        self._ecount       = 0

    @classmethod
    def vhash(cls, imp):
        return hashlib.md5(str(imp['video_id'])+imp['user_id']+str(imp['line_item_id'])).hexdigest()

    def accumulate(self, imps):
        self._ecount += 1
        for e in imps:
            self._ctr[e[0]] += 1

    def __str__(self):
        cs = '|'.join([k + '=' + str(v) for (k,v) in self._ctr.iteritems()])
        return self._url+':'+self._user_id+':'+self._line_item_id+':'+cs

    def dump_ib_tsv(self, hour_slot, outf):
        user_id = self._user_id != '_' and self._user_id or self.null_value
        outf.write("%s\n" % "\t".join([hour_slot.strftime("%Y-%m-%d %H:%M:%S"),
                                       user_id,
                                       str(self._line_item_id),
                                       "0",
                                       "0",
                                       self._page[0],              # page_id
                                       self._page[1],              # video_id
                                       self._page[4],              # player_height
                                       self._page[5]] +            # player_width
                                      [ str(self._ctr[c]) for c in self.counter_label ]
                                      ))



class AdServRollup(BoltBase):
    def __init__(self, shardid, *args, **kwargs):
        super(AdServRollup, self).__init__(*args, **kwargs)
        self._shardid = shardid
        self._imp_count = 0
        self._epoch = datetime.utcfromtimestamp(0)
        self._store = collections.defaultdict(dict)
        self._lasthr = None
        self._rollup_win   = kwargs.get("window", 3600)
        self._time_overlap = kwargs.get("overlap", 15*60)
        self._page_lookup = {}
        self._load_page_data()
        self._outf = open("infobright-output." + str(self._shardid) + ".tsv", "w")
                 
    def _load_page_data(self):
        pagedataf = gzip.GzipFile("page_data.tsv.gz", "r")
        cline = 0
        roll_log.warn('Importing webpage data file')
        for line in pagedataf:
            f = line.strip('\n').split('\t')
            cline += 1
            if (len(f) != 6):
                continue
            self._page_lookup[f[2]] = f
        roll_log.warn('Completed webpage data file import [%d] entries' % (len(self._page_lookup)))

    def should_push_rollup(self, ts):
        return (ts > self._lasthr + timedelta(seconds=self._rollup_win) + timedelta(seconds=self._time_overlap))
    
    def push_rollup(self, ts):
        d = self._lasthr
        roll_log.warn('Writing rollup [%s] @ %s [%s]  ImpCount: %d  ObjCount: %d' % (self._shardid, d.strftime("%Y-%m-%d %H:%M:%S"), ts.strftime("%Y-%m-%d %H:%M:%S"), self._imp_count, len(self._store[self._lasthr])))
        for vui in self._store[self._lasthr].values():
            vui.dump_ib_tsv(self._lasthr, self._outf)
        del self._store[self._lasthr]
        self._lasthr = self._lasthr + timedelta(hours=1)
        

    def process(self, imp):
        self._imp_count += 1
        if (imp['response'] != 'True'): return (False, None)
        if ((self._imp_count%50) == 0):
            roll_log.debug('[%s]  %07d   IMP: %s  SEQ: %d   EVT:%s' % 
                           (self._shardid, self._imp_count, imp["impression_id"], imp['sequence'], ':'.join((x[0] for x in imp['event_list']))))
        ts = datetime.strptime(imp["timestamp_str"], "%Y-%m-%d %H:%M:%S")
        hr_slot = datetime(ts.year, ts.month, ts.day, ts.hour)
        if (self._lasthr is None):
            self._lasthr = hr_slot
        if (self.should_push_rollup(ts)):
            self.push_rollup(ts)
        imp.setdefault('user_id', '_')
        if (imp['url'] not in self._page_lookup):
            roll_log.warn("url not found in page data; %s" % imp['url'])
            return (False, None)
        imp['page'] = self._page_lookup[imp['url']]
        imp['video_id'] = imp['page'][1]
        vhash = VideoUserLineItem.vhash(imp)
        if vhash not in self._store[hr_slot]:
            self._store[hr_slot][vhash] = VideoUserLineItem(imp)
        self._store[hr_slot][vhash].accumulate(imp['event_list'])
        return (False,None)

    
if __name__ == "__main__":
    import boto
    import os
    import time
    from streaming.kinesisbolt import KinesisSpout
    from streaming.server import kinesis_spawn

    source_stream = "AdsQueryCoalesced"
    def runner(shardid, **kwargs):
        kinspout = KinesisSpout(source_stream, shardid)
        adsrollup = AdServRollup(shardid)
        kinspout.addsink(adsrollup)
        try:
            kinspout.run()
        except KeyboardInterrupt:
            kinspout.shutdown()
            raise

    logging.basicConfig()
    roll_log.setLevel(logging.INFO)
    opts = collections.defaultdict(bool)
    argv = sys.argv[1:]
    cmd_line_setters = {
        's' : ('spawn', None),
        'S' : ('shardid', (lambda x: x)),
        'E' : ('emulation', None),
        'V' : ('debug', None),
        'N' : ('nosink', None),
        'I' : ('interactive', None)
        }

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

    if (opts['emulation']):
        from tools import energeia
        boto.connect_kinesis = (lambda: energeia.EnergeiaClient())
    if (opts['debug']):
        roll_log.setLevel(logging.DEBUG)

    if (opts['spawn']):
        childargs = filter((lambda x: x!='-s'), sys.argv[:])
        kinesis_spawn(source_stream, childargs, wait=opts['interactive'], **opts)


    elif (opts['shardid']):
        shardid = opts['shardid']
        del opts['shardid']
        logging.info('Child process [%d] assigned shard ID:', (os.getpid(), shardid))
        try:
            runner(shardid, **opts)
        except KeyboardInterrupt:
            print 'Keyboard Interrupt child:', os.getpid()
        sys.exit(0)

    

        
