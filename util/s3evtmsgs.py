from datetime import datetime, timedelta
import calendar
import boto
import time
import re
import io
import logging
import collections
import concurrent.futures
import threading
import sys
import json
import os
import msgpack
import getpass
import hashlib
from streaming import msgmodel
from streaming.util import SegRanges
from streaming.bolt import SpoutBase


S3_MOD_STRFRMT = '%Y-%m-%dT%H:%M:%S.000Z'
url_regex = re.compile(r'^[\d\.]+[\s\-]+\[.+\] \"GET (\/(query_line_item|(?P<reg>event\/register))\S+).+$')
sub_imp_regex = re.compile(r'(impression_id\=)[a-z0-9\_]+')


def time_snap_5minute(thistime):
    return datetime(thistime.year, thistime.month, thistime.day, thistime.hour, (thistime.minute - (thistime.minute%5)))

    

        


class S3EventTimeMap(object):
    def __init__(self):
        self._local_cache_file = "/tmp/s3evtinfo.cache.{0}.json".format(getpass.getuser())
        self._max_timestamp = calendar.timegm((2013,12,31,0,0,0))
        self._min_timestamp = int(time.time())
        self._s3map = {}
        self._cache_reload()
        print 'Earliest time stamp: ', datetime.utcfromtimestamp(self._min_timestamp)
        bucket_name = "affine-hadoop-input"
        self._s3_conn = boto.connect_s3()
        self._bucket = self._s3_conn.get_bucket(bucket_name)
        if (time.time() > (self._max_timestamp + (25*3600))):
            self._s3_scan()
            self._cache_save()

    def _cache_reload(self):
        if (os.path.exists(self._local_cache_file)):
            with open(self._local_cache_file, "r") as fcache:
                self._s3map = json.load(fcache)
                for segs in self._s3map.values():
                    self._max_timestamp = max(self._max_timestamp, reduce(lambda x,y: max(x, y[1]), segs, 0))
                    self._min_timestamp = min(self._min_timestamp, reduce(lambda x,y: max(x, y[1]), segs, 0))

    def _cache_save(self):
        with open(self._local_cache_file, "w") as fcache:
            json.dump(self._s3map, fcache)

    def get_instances(self, dtime):
        tx = calendar.timegm(dtime.timetuple())
        vinsts = []
        for (inst, segs) in self._s3map.items():
            for s in segs:
                if ((tx >= s[0]) and (tx <= s[1])):
                    vinsts.append(inst)
                    break
        return vinsts

    def unarchive(self, tstart, tend):
        dt = time_snap_5minute(tstart) + timedelta(minutes=5)
        while (dt <= tend):
            t0 = calendar.timegm(dt.timetuple())
            instv = []
            for (inst, segs) in self._s3map.items():
                for s in segs:
                    if ((t0 >= s[0]) and (t0 <= s[1])):
                        keyname = "ads_servers/{0}/evtmsg/{1}_{2:02d}/{3:02d}/eip_{4:02d}_{5:02d}.mpk".format(
                            inst, (dt.year-2000), dt.month, dt.day, dt.hour, dt.minute)
                        key = self._bucket.get_key(keyname)
                        if (key):
                            print key.name, key.storage_class, key.ongoing_restore
                            if (not key.ongoing_restore):
                                try:
                                    key.restore(days=5)
                                except:
                                    pass
                            key.close()
                            del key
            dt += timedelta(minutes=5)

        

    def get_keynames(self, tstart, tend):
        if (tend is None):
            tend = datetime.utcnow()
        if (tstart is None):
            tstart = datetime.utcnow() - timedelta(days=7) - timedelta(hours=5)
        dt = time_snap_5minute(tstart) + timedelta(minutes=5)
        while (dt <= tend):
            t0 = calendar.timegm(dt.timetuple())
            instv = []
            for (inst, segs) in self._s3map.items():
                for s in segs:
                    if ((t0 >= s[0]) and (t0 <= s[1])):
                        keyname = "ads_servers/{0}/evtmsg/{1}_{2:02d}/{3:02d}/eip_{4:02d}_{5:02d}.mpk".format(
                            inst, (dt.year-2000), dt.month, dt.day, dt.hour, dt.minute)
                        for itry in xrange(6):
                            try:
                                key = self._bucket.get_key(keyname)
                            except:
                                if (itry == 5):
                                    print "too many boto retries... aborting"
                                    raise
                                else:
                                    print "retry", itry
                                    time.sleep(0.1)
                            else:
                                if (key):
#                            print inst, s[0], s[1]
#                            print key.name, key.storage_class
                                    key.close()
                                    del key
                                    instv.append(keyname)
                                    break
            yield (dt,instv)
            dt += timedelta(minutes=5)

    def _s3_scan(self):
        #
        #  Scans S3 bucket for new evtmsg files after specified timestamp
        #  Returns dict keyed instance-names with arrays of timespans
        #
        frex = re.compile(r".+evtmsg/(\d{2})_(\d{2})/(\d{2})/eip_(\d{2})_(\d{2})\.mpk")
        workmap = {}
        for (inst,segs) in self._s3map.items():
            workmap[inst] = SegRanges((5*50), segments=segs)
        u_start = self._max_timestamp + 1
        prefix = "ads_servers/"

        for key in self._bucket.list(prefix=prefix, delimiter="/"):
            instname = os.path.basename(key.name.strip("/"))
            if (not re.match(r"^ads-([\da-f]{8}|\d{10})$", instname)): continue
            prefix2 = prefix + "{0}/evtmsg/".format(instname)
            for dkey in self._bucket.list(prefix=prefix2, delimiter="/"):
                (yy,mm) = map(int, os.path.basename(dkey.name.strip("/")).split("_"))
                YY = yy + 2000
                u_mon1 = calendar.timegm((YY, mm, calendar.monthrange(YY,mm)[1], 23, 59, 59))
                if (u_mon1 >= u_start):
                    prefix3 = prefix2 + "{0}_{1:02d}/".format(yy,mm)
                    for ekey in self._bucket.list(prefix=prefix3):
                        nn = frex.match(ekey.name)
                        if (nn):
                            dtuple = map(int, nn.groups())
                            fts = calendar.timegm(datetime(dtuple[0]+2000, *dtuple[1:]).timetuple())
                            if (fts >= u_start):
                                if (instname not in workmap):
                                    workmap[instname] = SegRanges((5*60))
#                                print ekey.name, ekey.storage_class
                                workmap[instname].add(fts)

        self._s3map = {}
        m1 = 0
        for (inst,segs) in workmap.items():
            self._s3map[inst] = [ s for s in segs ]
            m1 = max(m1, reduce(lambda x,y: max(x, y[1]), segs, 0))
        self._max_timestamp = m1
        print 'Completed S3 Scan'






class S3KeyEvtmsgIterator(object):
    _DEFAULT_BUCKET = "affine-hadoop-input"
    def __init__(self, keyname, blob=None, bucket=None, *argv, **kwargs):
        self._iobuf = None
        self._keyname = keyname
        self._blob = blob
        if (self._blob is None):
            bucket_name = (bucket, self._DEFAULT_BUCKET)[bucket is None]
            sbucket = boto.connect_s3().get_bucket(bucket_name)
            s3key = sbucket.get_key(keyname)
            try:
                self._blob = s3key.get_contents_as_string()
            except:
                print "Failed to read ", keyname
                pass
            s3key.close()
        if (self._blob):
            self._iobuf = io.BytesIO(self._blob)
        self._unpacker = msgpack.Unpacker()

    def __iter__(self):
        return self.next()

    def next(self):
        if (self._iobuf):
            while (True):
                chunk = self._iobuf.read(2048)
                if (len(chunk) == 0): break
                self._unpacker.feed(chunk)
                for rec in self._unpacker:
                    yield rec
        
        
    def iterator(self):
        while (True):
            chunk = self._iobuf.read(2048)
            if (len(chunk) == 0): break
            self._unpacker.feed(chunk)
            for rec in self._unpacker:
                yield rec



def s3_fetcher(bucket_name, keyname):
    bucket = boto.connect_s3().get_bucket(bucket_name)
    s3key = bucket.get_key(keyname)
    blob = s3key.get_contents_as_string()
    return (keyname, blob)



class S3EvtmsgIterator(object):
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgIterator, self).__init__()
        self._tstart = time_snap_5minute(tstart)
        self._tend = time_snap_5minute(tend)
        self._s3map = S3EventTimeMap()
        self._nslot = kwargs.get('nslot', 1)
        self._islot = kwargs.get('islot', 0)
        self._log = kwargs.get("logger", logging.getLogger("__main__"))

    def __iter__(self):
        return self.next()

    def next(self):
        for (kdate, s3keynames) in self._s3map.get_keynames(self._tstart, self._tend):
            for s3keyname in s3keynames:
                hk = int(hashlib.md5(s3keyname).hexdigest()[-6:], 16)
                if ((hk % self._nslot) == self._islot):
                    s3file = S3KeyEvtmsgIterator(s3keyname)
                    for rec in s3file:
                        yield rec





    
class S3EvtmsgSpout(SpoutBase):
    _S3_BUCKET = "affine-hadoop-input"
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgSpout, self).__init__()
        self._tstart = time_snap_5minute(tstart)
        self._tend = time_snap_5minute(tend)
        self._s3map = kwargs.get("s3map") or S3EventTimeMap()
        self._log = kwargs.get("logger", logging.getLogger("__main__"))

    def _get_next_event(self):
        tfilemark = self._tstart
        for (kdate, keynames) in self._s3map.get_keynames(self._tstart, self._tend):
            t0 = time.time()
            rec_iterators = []
            ftv = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(keynames)) as executor:
                ftv = [ executor.submit(s3_fetcher, self._S3_BUCKET, keyname) for keyname in keynames ]

            bytes = 0
            for ft in concurrent.futures.as_completed(ftv):
                (keyname, blob) = ft.result()
                bytes += len(blob)
                rec_iterators.append(S3KeyEvtmsgIterator(keyname, blob=blob).iterator())
            self._log.info('{2} Files: {3} Fetched: {0:.0f} MB  Elap:{1:.2f}s'.format(
                    (float(bytes)/(1024*1024)), (time.time() - t0), kdate, len(keynames)))
            tmark = kdate - timedelta(minutes=5)
            while (tmark < kdate):
                for iz in rec_iterators:
                    cur_tstamp = None
                    while (True):
                        try:
                            rawrec = iz.next()
                        except StopIteration:
                            print 'Exhausted'
                            rec_iterators.remove(iz)
                            break
                        r1 = rawrec['record']
                        evt = msgmodel.expand(r1)
                        if (cur_tstamp != evt["timestamp"]):
                            cur_tstamp = evt["timestamp"]
                            dt = datetime.strptime(cur_tstamp, "%Y-%m-%d %H:%M:%S")
                        yield evt
                        if (dt > tmark): break
                tmark += timedelta(seconds=1)


        
class S3EventTimeMap2(object):
    _BUCKET_NAME = "affine-streaming-data"
    def __init__(self):
        self._s3_conn = boto.connect_s3()
        self._bucket = self._s3_conn.get_bucket(self._BUCKET_NAME)
    
    def get_keynames(self, tstart, tend):
        dt = time_snap_5minute(tstart)
        while (dt <= tend):
            keypref = "ads_servers/evtmsg/{0:02d}_{1:02d}/{2:02d}/{3:02d}/{4:02d}/".format((dt.year-2000),
                                                                                           dt.month,
                                                                                           dt.day,
                                                                                           dt.hour,
                                                                                           dt.minute)
            keyv = []
            for s3key in self._bucket.list(prefix=keypref):
                keyv.append(s3key.name)
                s3key.close()
                del s3key
            yield (dt, keyv)
            dt += timedelta(minutes=5)



class S3EvtmsgIterator2(object):
    _BUCKET_NAME = "affine-streaming-data"
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgIterator2, self).__init__()
        self._tstart = time_snap_5minute(tstart)
        self._tend = time_snap_5minute(tend)
        self._s3map = S3EventTimeMap2()
        self._nshard = kwargs.get('nshard', 1)
        self._ishard = kwargs.get('ishard', 0)
        self._log = kwargs.get("logger", logging.getLogger("__main__"))

    def __iter__(self):
        return self.next()

    def next(self):
        for (kdate, s3keynames) in self._s3map.get_keynames(self._tstart, self._tend):
            for s3keyname in s3keynames:
                print kdate, s3keyname
                s3file = S3KeyEvtmsgIterator(s3keyname, bucket=self._BUCKET_NAME)
                for rec in s3file:
                    if (self._nshard != 1):
                        ii = rec["record"].get("ii")
                        if ((ii) and (int(ii.split("_")[0][-6:],16)%self._nshard == self._ishard)):
                            yield rec["record"]
                    else:
                        yield rec["record"]
    


class S3EvtmsgSpout2(SpoutBase):
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgSpout2, self).__init__()
        self._tstart = time_snap_5minute(tstart)
        self._tend   = time_snap_5minute(tend)
        self._nshard = kwargs.get('nshard', 1)
        self._ishard = kwargs.get('ishard', 0)
        self._s3iter = S3EvtmsgIterator2(self._tstart, self._tend, ishard=self._ishard, nshard=self._nshard)
        self._log    = kwargs.get("logger", logging.getLogger("__main__"))

    def _get_next_event(self):
        for rec in self._s3iter:
            yield msgmodel.expand(rec)




class S3KeyEvtmsgIterator3(object):
    def __init__(self, keyname, bucket_name, *argv, **kwargs):
        sbucket = boto.connect_s3().get_bucket(bucket_name)
        s3key = sbucket.get_key(keyname)
        for itry in xrange(3):
            try:
                blob = s3key.get_contents_as_string()
            except:
                if (itry == 2):
                    raise
            else:
                break
        self._iobuf = io.BytesIO(blob)
        s3key.close()
        self._unpacker = msgpack.Unpacker()
        
    def __iter__(self):
        return self.next()

    def next(self):
        while (True):
            chunk = self._iobuf.read(10240)
            if (len(chunk) == 0): break
            self._unpacker.feed(chunk)
            for rec in self._unpacker:
                yield rec
        



class S3EvtmsgCombinedIterator(object):
    _S3_BUCKET = "affine-streaming-data"
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgCombinedIterator, self).__init__()
        self._tstart = time_snap_5minute(tstart)
        self._tend = time_snap_5minute(tend)
        self._s3map = S3EventTimeMap2()
        self._nshard = kwargs.get('nshard', 1)
        self._ishard = kwargs.get('ishard', 0)
        self._log = kwargs.get("logger", logging.getLogger("__main__"))

    def next(self):
        max_time = datetime(2014,1,1,0,0)
        peek_ahead = []
        for (kdate, keynames) in self._s3map.get_keynames(self._tstart, self._tend):
            rec_iterators = [ S3KeyEvtmsgIterator3(keyname, self._S3_BUCKET).next() for keyname in keynames ]
            tmark = kdate - timedelta(minutes=5)
            while (tmark < kdate):
                while (len(peek_ahead)):
                    yield peek_ahead.pop(0)
                for iz in rec_iterators:
                    cur_tstamp = None
                    while (True):
                        try:
                            rawrec = iz.next()
                        except StopIteration:
                            #print 'Exhausted'
                            rec_iterators.remove(iz)
                            break
                        r1 = rawrec['record']
                        evt = msgmodel.expand(r1)
                        if (self._nshard > 1):
                            ii = evt['impression_id']
                            if (int(ii.split("_")[0][-6:],16)%self._nshard != self._ishard): continue
                        if (evt["timestamp"] != cur_tstamp):
                            cur_tstamp = evt["timestamp"]
                            dt = datetime.strptime(cur_tstamp, "%Y-%m-%d %H:%M:%S")
                            if (dt > tmark):
                                peek_ahead.append(evt)
                                break
                        yield evt
                tmark += timedelta(seconds=1)




class S3EvtmsgSpout3(S3EvtmsgCombinedIterator, SpoutBase):
    def __init__(self, tstart, tend, *argv, **kwargs):
        super(S3EvtmsgSpout3, self).__init__(tstart=tstart, tend=tend, *argv, **kwargs)

    def _get_next_event(self):
        return self.next()


                            
                    
                        
    

if __name__ == "__main__":
    from streaming.bolt import BoltBase
    from streaming.adservcoalesce import AdServCoalesce

#    sbucket = boto.connect_s3().get_bucket("affine-streaming-data")
    tstart = datetime(2015,1,4,0,0)
    tend   = datetime(2015,1,4,0,5)
    cnt = 0
    t0 = time.time()
    # sp = S3EvtmsgSpout3(tstart, tend)
    # for e in sp._get_next_event():
    #     cnt += 1

    # s3map = S3EventTimeMap2()
    # unpacker = msgpack.Unpacker()
    # for (dt,keynames) in s3map.get_keynames(tstart, tstart):
    #     siter = S3KeyEvtmsgIterator3(keynames[0], "affine-streaming-data")
    #     for e in siter:

    # print "Run from ", tstart, " to ", tend
    # print "Elapsed {0:.0f}".format((time.time() - t0))
    # print "Event Count:", cnt
    # sys.exit(0)
        

    class TesterBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(TesterBolt, self).__init__(*args, **kwargs)
            self._view_count = 0
            self._last_time_str = None
            self._last_mark = 0
            self._t0 = time.time()

        def process(self, rec):
            elist = rec['event_list']
            self._view_count += any(((etype == 'view') for (etype,timestamp) in elist))
            if (rec['timestamp'] != self._last_time_str):
                self._last_time_str = rec['timestamp']
                dtime = datetime.strptime(rec["timestamp"], "%Y-%m-%d %H:%M:%S")
                self._last_ts = calendar.timegm(dtime.timetuple())
                if ((self._last_ts > self._last_mark) and ((self._last_ts % 600) == 0)):
                    self._last_mark = self._last_ts
                    print dtime, "{0:.0f} secs".format((time.time() - self._t0))


    tester = TesterBolt()
    adscoalesc = AdServCoalesce(timewindow=(4*60))
    adscoalesc.addsink(tester)
#    s3spout = S3EvtmsgSpout2(tstart, tend, ishard=0, nshard=1)
    s3spout = S3EvtmsgSpout3(tstart, tend, ishard=0, nshard=10)
    s3spout.addsink(adscoalesc)
    t0 = time.time()
    s3spout.run()
    print "Run from ", tstart, " to ", tend
    print "Elapsed {0:.0f}".format((time.time() - t0))
    print "Views : ", tester._view_count
            

    
    



















