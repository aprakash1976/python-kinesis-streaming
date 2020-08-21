from streaming.util.s3evtmsgs import S3EventTimeMap, S3EvtmsgIterator
from datetime import datetime, timedelta
from streaming import msgmodel
from collections import Counter, defaultdict
from affine.model import session, WebPage, WebPageLabelResult, Label, LineItem, Campaign
from streaming.util import snaphour
from streaming.util import datamodel
import hashlib
import time
import sys
import MySQLdb
import os.path

def get_labels(page_ids):
    cols = [
        WebPageLabelResult.page_id,
        WebPageLabelResult.label_id,
        Label.name,
        Label.confidence,
        ]
    query = session.query(*cols)
    query = query.filter(WebPageLabelResult.page_id.in_(page_ids))
    query = query.join(WebPageLabelResult.label)
    query = query.order_by(Label.id)

    vresult = []
    for row in query:
        page_id, label_data = row[0], row[1:]
        vresult.append((page_id, label_data))
    return vresult





class CDPostLabel(object):
    _VIEWABILITY_LABEL_ID = 7840
    _MYSQL_NULL = "\\N"
    def __init__(self, dstart, dend, *args, **kwargs):
        super(CDPostLabel, self).__init__(*args, **kwargs)
        self._dstart = dstart
        self._dend   = dend
        self._webpagelabel_cache = {}
        self._s3msgiter = S3EvtmsgIterator(dstart, dend, nslot=1, islot=0)
        self._ctr = Counter()
        self._unsafe_label_ids = {}
        self._webpagelabel_cache = {}
        self._outdir  = "."
        h0 = snaphour(self._dstart)
        h1 = snaphour(self._dend) - timedelta(hours=1)
        self._outfile = os.path.join(self._outdir, 
                                     "cdlineitem_{0}{1:02d}{2:02d}{3:02d}_{4}{5:02d}{6:02d}{7:02d}.tsv".format(h0.year, h0.month, h0.day, h0.hour,
                                                                                                               h1.year, h1.month, h1.day, h1.hour))
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

    def _dumptofile(self, currhour, metrics):
        fout = open(self._outfile, "a")
        tdbstr = currhour.strftime("%Y-%m-%d %H:%M:%S")
        for (liid, counters) in metrics.items():
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
        fout.close()

    def run(self):
        currhour = None
        ctr = Counter()
        for rawrec in self._s3msgiter:
            ctr['all'] += 1
            rrec = rawrec['record']
            if (rrec.get('et', '') != 'cd-event'): continue
            rec = msgmodel.expand(rrec)
            dtime = datetime.strptime(rec["timestamp"], "%Y-%m-%d %H:%M:%S")
            if (dtime < self._dstart): continue
            chour = snaphour(dtime)
            if (currhour is None) or (chour > currhour):
                if (currhour):
                    print "Processed hour in {0:.0f} sec   Stats: {1}".format((time.time()-t0), str(ctr))
                    self._dumptofile(currhour, metrics)
                t0 = time.time()
                metrics = {}
                currhour = chour
                ctr = Counter()
            ctr['cdevent'] += 1
            if ('url' not in rec) or ('event_type' not in rec) or ('line_item_id' not in rec):
                ctr['malformed'] += 1
                continue
            line_item_id = rec['line_item_id']
            if (line_item_id not in metrics):
                metrics[line_item_id] = [0]*len(self._cntrmap)
            if (line_item_id not in self._unsafe_label_ids):
                self._unsafe_label_ids[line_item_id] = set(datamodel.get_brand_unsafe_label_ids([line_item_id]))
            metrics[line_item_id][self._cntrmap[rec['event_type']]] += 1
            if (rec['event_type'] != 'view'): continue
            ctr['impression'] += 1
            hurl = hashlib.md5(rec['url']).hexdigest()
            if (hurl not in self._webpagelabel_cache):
                self._webpagelabel_cache[hurl] = None
                ctr['urllookup'] += 1
                wp = WebPage.by_url(rec['url'], session=session)
                if (wp):
                    ctr['urlknown'] += 1
                    vlabels = get_labels([wp.id])
                    self._webpagelabel_cache[hurl] = set((label[0] for (page_id, label) in vlabels))
            if (self._webpagelabel_cache[hurl] is not None):
                ctr['knownpage'] += 1
                metrics[line_item_id][self._cntrmap['knownimp']] += 1
                metrics[line_item_id][self._cntrmap['brandsafe']] += (0,1) [ not self._unsafe_label_ids[line_item_id] & set(self._webpagelabel_cache[hurl]) ]
                metrics[line_item_id][self._cntrmap['viewableimp']] += (0,1)[ (self._VIEWABILITY_LABEL_ID in self._webpagelabel_cache[hurl]) ]



class CDPostScheduler(object):
    _MAX_PROCESS = 5
    _MAX_HOUR_RUN = 24
    def __init__(self, *argv, **kwargs):
        self._maxproc = kwargs.get('max_process', self._MAX_PROCESS)
        self._conn = MySQLdb.connect(host="db-master.dev.set.tv", db="live", passwd="1977neverUmind2013", user="prod")
        self._cursor = self._conn.cursor()
        self._growtime()

    def _growtime(self):
        self._cursor.execute("SELECT MAX(date) FROM cd_post_schedule")
        tlast = self._cursor.fetchone()[0]
        tcur = datetime(tlast.year, tlast.month, tlast.day, tlast.hour) + timedelta(hours=1)
        tnow = datetime.utcnow() - timedelta(hours=5)
        while (tcur < tnow):
            print tcur
            self._cursor.execute("INSERT IGNORE INTO cd_post_schedule (`date`) VALUES ('{0}');".format(str(tcur)))
            tcur += timedelta(hours=1)
        self._conn.commit()

    def _contigous_timerange(self, timeseries):
        tfirst = timeseries[0]
        tend   = timeseries[0]
        for tcur in timeseries[1:self._MAX_HOUR_RUN]:
            if (tcur != tend + timedelta(hours=1)): break
            tend = tcur
        return tfirst, tend
        

    def daterange(self):
        self._cursor.execute("SELECT date FROM cd_post_schedule WHERE last_started_at IS NULL ORDER BY date ASC;")
        null_times = self._cursor.fetchall()
        if (len(null_times) > 0):
            null_times = [x[0] for x in null_times]
            return self._contigous_timerange(null_times)

        self._cursor.execute("SELECT date FROM cd_post_schedule WHERE (last_finished_at > last_started_at) AND (process_count < {0}) ORDER BY process_count, date ASC;".format(self._MAX_PROCESS))
        due_times = self._cursor.fetchall()
        if (len(due_times) > 0):
            due_times = [x[0] for x in due_times]
            return self._contigous_timerange(due_times)


    def running(self):
        self._cursor.execute("SELECT date FROM cd_post_schedule WHERE (last_started_at > last_finished_at) OR ((last_started_at is not NULL) and (last_finished_at is NULL));")
        vrunning = self._cursor.fetchall()
        return [v[0] for v in vrunning]

    def markfinished(self, tfinished):
        self._cursor.execute("UPDATE cd_post_schedule SET last_finished_at = NOW(), process_count = process_count+1  WHERE date = '{0}';".format(tfinished))
        self._conn.commit()

    def markstarted(self, tstarted):
        self._cursor.execute("UPDATE cd_post_schedule SET last_started_at = NOW() WHERE date = '{0}';".format(tstarted))
        self._conn.commit()

    def status(self, dtime):
        self._cursor.execute("SELECT process_count, last_started_at, last_finished_at FROM cd_post_schedule WHERE date = '{0}';".format(dtime))
        if (self._cursor.rowcount != 1): return None
        (process_count, last_started_at, last_finished_at) = self._cursor.fetchone()
        if (process_count >= self._MAX_PROCESS):
            state = 'COMPLETE'
        elif ((last_started_at != None) and (last_finished_at == None)) or (last_started_at > last_finished_at):
            state = 'RUNNING'
        else:
            state = 'IDLE'
        return state, process_count, last_started_at, last_finished_at
        
        
        
        

if __name__ == "__main__":
#    (dstart, dend) = cdsched.daterange()
#    dstart = datetime(2014, 12, 29, 1)
    dstart = datetime(2014, 9, 1, 0)
#    dstart = datetime(2014, 10, 5, 12)
    dend = dstart + timedelta(hours=24) + timedelta(minutes=6)
#    dend = dstart + timedelta(hours=5)
    print 'Start:   ', dstart, '    End:    ', dend
    cdpostlabeller = CDPostLabel(dstart, dend)
    cdpostlabeller.run()







