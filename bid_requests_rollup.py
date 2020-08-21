import logging
import sys
import os
import time
import re
import signal
import threading
import calendar
from datetime import datetime, timedelta
from collections import Counter
from hashlib import sha1
from affine import normalize_url
from streaming.util import shardid2index, snaphour,\
    args_parser, TCounter
from streaming.kinesisbolt import KinesisSpoutUnbundler
from streaming.bolt import BoltBase
from affine.log import configure_logging
from mpkinesismgr import MPKinesisMgr
from affine.normalize_url import parse_url

args_cfg = {
    "idx": {
        "tovalue": int
    },
    "reset": {
        "switch": True,
        "default": False
    },
    "start": {
        "tovalue": str
    }
}


def snap_minute(dtime, interval):
    minute = dtime.minute - (dtime.minute % interval)
    return datetime(dtime.year, dtime.month, dtime.day, dtime.hour, minute)


def snap_hour(dtime):
    return datetime(dtime.year, dtime.month, dtime.day, dtime.hour)


def epoch_hour(dtime):
    dt = dtime - datetime(2000, 1, 1)
    return dt.days * 24 + dt.seconds // 3600


class Dumper(threading.Thread):
    def __init__(self, shardindex, etime, page_req, queueloader, **kwargs):
        super(Dumper, self).__init__()
        self._log = kwargs.get("log")
        self._shardindex = shardindex
        self._etime = etime
        self._page_req = page_req
        self._queueloader = queueloader
    
    def run(self):
        tdump = self._etime
        tdump += timedelta(minutes=5) - timedelta(hours=1)
        tdump = snaphour(tdump)
        tdbstr = tdump.strftime("%Y-%m-%d %H:%M:%S")
        t0 = time.time()
        self._log.info("Starting dump @ {2}   Table sizes: bid_requests[{0}] "
                       "queueloader[{1}]".format(len(self._page_req),
                                                 len(self._queueloader),
                                                 self._etime))
        #
        rex = re.compile(r'^p\-(\d+)$')
        opath = BidRequests.output_filename(BidRequests._TABLE_NAMES[0], tdump, 
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((content_id, country_code), count) in self._page_req.items():
                mo = rex.match(content_id)
                if (mo):
                    content_id = mo.group(1)
                    fout.write("{0}\t{1}\t{2}\t{3}\n".format(tdbstr,
                                                             content_id,
                                                             count,
                                                             country_code))
        #
        opath = BidRequests.output_filename(BidRequests._TABLE_NAMES[1], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((shard_key, country_code), dict_values) in self._queueloader.items():
                domain = dict_values['domain']
                count = dict_values['count']
                try:
                    url = parse_url(dict_values['url'])
                    subdomain = normalize_url.domain_of_url(url,
                                                            with_subdomains=True)
                except:
                    self._log.warn("Skipped unparseable URL: {0}".format(url))
                    continue
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(shard_key,
                           url, domain, subdomain, country_code, count))
        #
        #
        rex = re.compile(r'^a\-(\d+)$')
        opath = BidRequests.output_filename(BidRequests._TABLE_NAMES[2], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((content_id, country_code), count) in self._page_req.items():
                mo = rex.match(content_id)
                if (mo):
                    content_id = mo.group(1)
                    fout.write("{0}\t{1}\t{2}\t{3}\n".format(tdbstr,
                                                             content_id,
                                                             count,
                                                             country_code))
        self._log.info("Dump completed:  elapsed: {0:.1f}s".format((time.time() - t0)))
        return


class BidRequests(BoltBase):
    _MYSQL_NULL = "\\N"
    _DOMAIN_MAXLEN = 128
    _DUMP_VERSION = 2
    _TABLE_NAMES = (
        "bid_requests_page",
        "queue_loader",
        "bid_requests_app"
    )

    def __init__(self, *args, **kwargs):
        super(BidRequests, self).__init__(*args, **kwargs)
        self._wrlock = threading.Lock()
        self._started = False
        self._log = kwargs.get("logger", logging.getLogger("__main__"))
        self._cntr = TCounter()
        self._shardid = kwargs.get("shard_id")
        self._bckfill = kwargs.get("back_fill")
        if ("logger" not in kwargs):
            self._log.setLevel(logging.INFO)
        if (not self._bckfill):
            self._shardindex = shardid2index(self._shardid)
        else:
            self._shardindex = None
        self._log.info("Shardindex {0}".format(self._shardindex))
        self._init_output_dirs()
        #
        self._tstart = kwargs.get("tstart",
                                  datetime.utcnow() - timedelta(days=1))
        self._tend = kwargs.get("tend", datetime.utcnow() +
                                timedelta(days=365 * 100))
        self._etime = datetime(2000, 1, 1)
        self._last_checkpt = self._etime
        self._current_time = None
        self._first_ts = None
        self._evtcount = 0
        self._dump_hour = 0
        self._domain_rex = re.compile("^http:\/\/(\w+\.)?(\w+\.\w{2,3})\/.*$")
        self._page_req = None
        self._queue_loader = {}
        self._counters_swap()
        self._log.info("bidrequests init complete")
        # must be a divisor of 60
        self._checkpoint_interval = 10
        self._restored_seqnum = None
        self._tdumper = None

    def _checkpoint(self, seqnum):
        if (self._bckfill):
            return
        pchkdir = os.path.join(os.getenv("HOME"),
                               "checkpoint",
                               "%04d" % self._shardindex)
        if (not os.path.exists(pchkdir)):
            os.makedirs(pchkdir)
        dtime = snap_hour(self._etime)
        chkptf = os.path.join(pchkdir, dtime.strftime("%y%m%d%H"))
        with open(chkptf, "w") as chkf:
            chkf.write("{0}\t{1}\n".format(seqnum,
                                           calendar.timegm(self._etime.timetuple())))
        self._log.info("Checkpoint @ {0} {1}".format(self._etime, chkptf))

    def _restore(self):
        pchkdir = os.path.join(os.getenv("HOME"),
                               "checkpoint",
                               "%04d" % self._shardindex)
        if (not os.path.exists(pchkdir)):
            os.makedirs(pchkdir)
        chkptfs = sorted(os.listdir(pchkdir))
        if (len(chkptfs)):
            chkptf = chkptfs[-1]
            if (re.match(r'\d{8}', chkptf)):
                ctime = datetime.strptime(chkptf, "%y%m%d%H")
                if ((datetime.utcnow() - ctime) <= timedelta(hours=23)):
                    with open(os.path.join(pchkdir, chkptf), "r") as sfin:
                        (seqnum, evtts) = sfin.readline().strip("\n").split("\t")
                        self._log.info("Valid checkpoint: {0}  Seqnum: {1}\
                                       Eventtime: {2}".format(
                                       chkptf, seqnum,
                                       datetime.utcfromtimestamp(int(evtts))))
                        self._restored_seqnum = seqnum

    def _counters_swap(self):
        self._wrlock.acquire()
        page_req = self._page_req
        queueloader = self._queue_loader
        self._page_req = Counter()
        self._queue_loader = {}
        self._wrlock.release()
        return (page_req, queueloader)

    @property
    def restored_seqnum(self):
        return self._restored_seqnum

    def _init_output_dirs(self):
        topdir = os.path.join(os.getenv("HOME"), "output")
        for table in self._TABLE_NAMES:
            dpath = os.path.join(topdir, table)
            if (not os.path.exists(dpath)):
                self._log.info("Creating output directory {0}".format(dpath))
                os.makedirs(dpath)
        chkptdir = os.path.join(os.getenv("HOME"), "checkpoint")
        if (not os.path.exists(chkptdir)):
            os.makedirs(chkptdir)

    def _fast_getdomain(self, url):
        if url is not None:
            mo = self._domain_rex.match(url)
            return (mo) and mo.group(2) or None
        else:
            return None

    def eob_callback(self, last_seqnum):
        if ((self._etime - self._last_checkpt) >= timedelta(hours=1)):
            self._last_checkpt = snap_hour(self._etime)
            if ((self._etime - self._first_ts) > timedelta(seconds=300)):
                self.dumper()
                self._restored_seqnum = None
                self._checkpoint(last_seqnum)

    def process(self, imp):
        self._start()
        if 'content_id' not in imp:
            return (False, None)
        self._evtcount += 1
        #
        #  time housekeeping
        #
        if self._current_time != imp.get('unix_timestamp'):
            self._current_time = imp.get('unix_timestamp')
            self._etime = datetime.utcfromtimestamp(self._current_time)
            if (self._first_ts is None):
                self._first_ts = self._etime
                self._last_checkpt = snap_hour(self._etime)

            if (self._bckfill):
                delta = self._etime - self._first_ts
                event_info = "{0}  event elap: {1:8.0f}    wall elap: {2:8.0f}"
                if ((self._etime.second % 30) == 0):
                    self._log.info(event_info.format(
                        self._etime,
                        delta.total_seconds(),
                        (time.time() - self._wall_t0)))
        if ((self._etime < self._tstart) or (self._etime > self._tend) or
                ('url' not in imp)):
            return (False, None)
        content_id = imp.get('content_id', 0)
        country_code = imp.get('country_code', 'US')
        if content_id is None:
            content_id = 0
        url = imp.get('url')
        domain = None
        if url is not None:
            domain = self._fast_getdomain(imp['url'])
            if (domain is None):
                domain = normalize_url.domain_of_url(imp['url'])
        if (domain is not None):
            domain = domain[:self._DOMAIN_MAXLEN]
        if content_id == 0:
            if url is not None and len(url) > 1:
                key = (sha1(url).hexdigest(), country_code)
                if key not in self._queue_loader:
                    self._queue_loader[key] = {'url': url,
                                               'domain': domain,
                                               'count': 1}
                else:
                    self._queue_loader[key]['count'] += 1
        else:
            page_grpkey = (content_id, country_code)
            self._page_req[page_grpkey] += 1
        return (False, None)

    def _start(self):
        if (not self._started):
            self._wall_t0 = time.time()
            self._started = True

    @classmethod
    def output_filename(cls, table_name, tnow, shardindex):
        fname = "ru.{0}.v{1}.".format(tnow.strftime("%Y-%m-%d-%H-%M-%S"),
                                      cls._DUMP_VERSION)
        fname += ("tsv", "{0}.tsv".format(shardindex))[shardindex is not None]
        return os.path.join(os.getenv("HOME"), "output", table_name, fname)

    def dumper(self):
        (page_req, queueloader) = self._counters_swap()
        if (self._tdumper):
            self._tdumper.join()
            self._tdumper = None
        self._tdumper = Dumper(self._shardindex, self._etime, page_req, 
                               queueloader, log=self._log)
        self._tdumper.start()


class BidRequestsMgr(MPKinesisMgr):

    def __init__(self, processname, *args, **kwargs):
        super(BidRequestsMgr, self).__init__(processname, *args, **kwargs)

    def TERM_handler(self, signum):
        self._log.warn("caught TERM signal .. shutting down")
        self._worker.shutdown()
        self._spout.shutdown()
        sys.exit(0)

    def main(self):
        (aoptions, otherargs) = args_parser(sys.argv[1:], args_cfg)
        configure_logging("{0}_{1:04d}.log".format(self._name,
                          int(aoptions.get('idx', os.getpid()))))
        self._log = logging.getLogger("__main__")
        self._log.info("inside main: pid={0} idx={1}".format(os.getpid(),
                       aoptions['idx']))
        self._log.info("options: {0}".format(aoptions))
        self._log.info("otherargs: {0}".format(otherargs))
        self._log.info("has TERM handler {0}".format(signal.getsignal(
                       signal.SIGTERM)))
        self._streamname = (a for a in otherargs if (
                            not a.startswith('-'))).next()
        self._set_streaminfo()
        self._shard = self._shards[aoptions['idx']]
        self._log.info("Shard info: {0}".format(self._shard))
        self._worker = BidRequests(logger=self._log,
                                   shard_id=self._shard['ShardId'])
        self._worker._restore()
        restored_seqnum = None
        start_at = aoptions.get("start", "LATEST")
        if (not aoptions["reset"]):
            if (self._worker.restored_seqnum or aoptions.get("seqnum")):
                start_at = "RESUME"
                restored_seqnum = aoptions.get("seqnum",
                                               self._worker.restored_seqnum)
                self._log.info("Restored seqnum = {0}".format(restored_seqnum))
        if (start_at not in ("LATEST", "RESUME", "TRIM_HORIZON")):
            self._log.error("Illegal kinesis start position: {0}".format(start_at))
            return None
        self._spout = KinesisSpoutUnbundler(self._streamname,
                                            self._shard['ShardId'],
                                            logger=self._log,
                                            cntr=self._worker._cntr,
                                            eob_cb=self._worker.eob_callback,
                                            last_seqnum=restored_seqnum,
                                            start_at=start_at)
        self._spout.addsink(self._worker)
        self._spout.run()
        self._log.info("Main exit")
        return None

if __name__ == "__main__":
    t = BidRequestsMgr("bid_requests_rollup")
    t.start()
    sys.exit(0)
