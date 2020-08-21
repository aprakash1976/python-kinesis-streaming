import logging
import sys
import os
import time
import re
import signal
import threading
import calendar
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from affine.retries import retry_operation
from streaming.util import shardid2index, snaphour,\
    args_parser, TCounter
from streaming.kinesisbolt import KinesisSpoutUnbundler
from streaming.bolt import BoltBase
from affine.log import configure_logging
from affine.model import LineItem, Campaign, session, Label,\
    Channel
from mpkinesismgr import MPKinesisMgr
from streaming.util import datamodel

args_cfg = {
    "idx": {
        "tovalue": int
    },
    "seqnum": {
        "tovalue": str
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


class EventRollup(BoltBase):
    _REASON_UNKNOWN_URL = 1
    _VIEWABILITY_LABEL_ID = 7840
    _MYSQL_NULL = "\\N"
    _BLOCKED_MEDIA_MAX_ROWS = 10
    _DOMAIN_MAXLEN = 128
    _DUMP_VERSION = 2
    _TABLE_NAMES = (
        "served_media",
        "blocked_media",
        "users",
        "labels",
        "line_item_metrics",
        "redis"
    )

    def __init__(self, *args, **kwargs):
        super(EventRollup, self).__init__(*args, **kwargs)
        self._wrlock = threading.Lock()
        self._dumplock = threading.Lock()
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
        #
        self._brand_unsafe_labels = {}
        self._event_set = ('view', 'firstQuartile', 'midpoint', 'thirdQuartile', 'complete', 'click', 'skip' )
        self._li_cntrmap = {'query': 0, 'view': 1, 'firstQuartile': 2,
                            'midpoint': 3, 'thirdQuartile': 4, 'complete': 5,
                            'click': 6, 'skip': 7, 'viewableimp': 8,
                            'brandsafe': 9, 'trues': 10, 'falses': 11,
                            'nones': 12, 'knownimp': 13}
        self._sp_cntrmap = {'view': 0, 'click': 1, 'firstQuartile': 2,
                            'midpoint': 3, 'thirdQuartile': 4, 'complete': 5,
                            'skip': 6, 'brandsafe': 7, 'viewableimp': 8,
                            'knownimp': 9}
        self._lb_cntrmap = {'view': 0, 'click': 1, 'firstQuartile': 2,
                            'midpoint': 3, 'thirdQuartile': 4, 'complete': 5,
                            'skip': 6}
        self._respmap = {'True': 'true', 'False': 'false', 'None': 'none'}
        self._li_respmap = {'true': 10, 'false': 11, 'none': 12}
        self._domain_rex = re.compile("^http:\/\/(\w+\.)?(\w+\.\w{2,3})\/.*$")
        self._lineiteminfo = {}
        self._labelinfo = {}
        self.load_personality_labels()
        self._served_media = None
        self._li_metrics = None
        self._blocked_media = None
        self._userbyli = None
        self._domain_reasons = None
        self._labels = None
        self._redis_info = None
        self._queue_loader = {}
        self._noncontext_liids = {}
        self._counters_swap()
        self._metadata_reset()
        self._log.info("eventrollup init complete")
        # must be a divisor of 60
        self._checkpoint_interval = 10
        self._restored_seqnum = None

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
        served_media = self._served_media
        blocked_media = self._blocked_media
        userbyli = self._userbyli
        labels = self._labels
        redisinfo = self._redis_info
        li_metrics = self._li_metrics
        self._served_media = {}
        self._blocked_media = set()
        self._li_metrics = {}
        self._userbyli = {}
        self._domain_reasons = Counter()
        self._labels = {}
        self._falselimit = Counter()
        self._redis_info = defaultdict(set)
        self._wrlock.release()
        return (served_media, blocked_media, userbyli,
                labels, redisinfo, li_metrics)

    def _metadata_reset(self):
        self._wrlock.acquire()
        for liid, values in self._lineiteminfo.items():
            if values is None:
                del self._lineiteminfo[liid]
        self._pageinfo = {}
        self._wrlock.release()

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

    def load_personality_labels(self):
        perslabels = retry_operation(Label.by_type, 'personality',
                                     sleep_time=2)
        self._PERS_LABELS = set(perslabels)
        session.close()

    def _setlineiteminfo(self, line_item_id):
        if (line_item_id not in self._lineiteminfo):
            lineitem = retry_operation(LineItem.get, [line_item_id],
                                       sleep_time=5)
            if (lineitem):
                campaign = retry_operation(Campaign.get,
                                           [lineitem.campaign_id],
                                           sleep_time=5)
                if (campaign):
                    self._lineiteminfo[line_item_id] = (campaign.id, int(
                        campaign.advertiser_id), lineitem.publisher_id or 0,
                        lineitem.is_contextual)
                    self._log.info("Added lineitem[{0}] campaign[{1}]".format(
                        line_item_id, campaign.id))
            session.close()
        li_warning = "cannot get LineItem and/or Campaign for line_item_id: {}"
        if (not self._lineiteminfo.get(line_item_id)):
            if (line_item_id not in self._lineiteminfo):
                self._log.warn(li_warning.format(line_item_id))
                self._lineiteminfo[line_item_id] = None
            return False
        return True

    def _fast_getdomain(self, url):
        if url is not None:
            mo = self._domain_rex.match(url)
            return (mo) and mo.group(2) or None
        else:
            return None

    def _evt2incr(self, elist, cmap):
        incr = [0] * len(cmap)
        for (etype, ts) in elist:
            if ((etype in cmap) and (etype != 'query')):
                incr[cmap[etype]] = 1
        return incr

    def eob_callback(self, last_seqnum):
        if ((self._etime - self._last_checkpt) >= timedelta(hours=1)):
            self._last_checkpt = snap_hour(self._etime)
            if ((self._etime - self._first_ts) > timedelta(seconds=300)):
                self._log.info("Start dump @ {0}".format(self._etime))
                self.dumper()
                self._restored_seqnum = None
                self._checkpoint(last_seqnum)

    def process(self, imp):
        self._start()
        self._evtcount += 1
        #
        #  time housekeeping
        #
        if self._current_time != imp.get('unix_timestamp'):
            self._current_time = imp.get('unix_timestamp')
            self._etime = datetime.utcfromtimestamp(self._current_time)
            if (self._first_ts is None):
                self._first_ts = self._etime
                self._last_checkpt = snap_minute(self._etime,
                                                 self._checkpoint_interval)
            if (self._bckfill):
                delta = self._etime - self._first_ts
                event_info = "{0}  event elap: {1:8.0f}    wall elap: {2:8.0f}"
                if ((self._etime.second % 30) == 0):
                    self._log.info(event_info.format(
                        self._etime,
                        delta.total_seconds(),
                        (time.time() - self._wall_t0)))
        '''
        if ((self._etime < self._tstart) or (self._etime > self._tend) or
                ('url' not in imp)):
            return (False, None)
        '''
        #
        #  event procesing/rollups
        #
        content_id = imp.get('content_id', None)
        domain = imp.get('domain', '')
        page_id = 0
        app_id = 0
        if content_id is not None:
            if 'p_' in content_id:
                page_id = content_id.split('_', 1)[1]
            elif 'a_' in content_id:
                app_id = content_id.split('_', 1)[1]
            elif 'p-' in content_id:
                page_id = content_id.split('-', 1)[1]
            elif 'a-' in content_id:
                app_id = content_id.split('-', 1)[1]
        if (domain is not None):
            domain = domain[:self._DOMAIN_MAXLEN]
        video_id = imp.get('video_id', 0)
        demo_id = imp.get('demo', 0)
        if demo_id is None:
            demo_id = 0
        if app_id > 0:
            media_type = 'app'
        elif video_id > 0:
            media_type = 'video'
        else:
            media_type = 'page'
        imp_liid = 0
        self._wrlock.acquire()

        events = imp.get("events", {})
        if events.get("no_match") is not None:
            no_liids = events.get("no_match")
            for no_liid in no_liids:
                if (not self._setlineiteminfo(no_liid)):
                    continue
                li_grp_key = (no_liid, demo_id)
                if li_grp_key not in self._li_metrics:
                    self._li_metrics[li_grp_key] = [0] * len(self._li_cntrmap)
                if content_id is not None:
                    #
                    # blocked pages
                    #
                    #falsegroup = (no_liid, page_id)
                    self._falselimit[no_liid] += 1
                    if (self._falselimit[no_liid] <= self._BLOCKED_MEDIA_MAX_ROWS):
                        if page_id != 0:
                            if (page_id not in self._pageinfo):
                                self._pageinfo[page_id] = [domain, 0]
                            self._pageinfo[page_id][1] = video_id
                            self._blocked_media.add((no_liid, page_id, media_type))
                        elif app_id != 0:
                            self._blocked_media.add((no_liid, app_id, media_type))
                    #
                    # line_item_metrics (false)
                    #
                    self._li_metrics[li_grp_key][self._li_respmap['false']] += 1
                    self._li_metrics[li_grp_key][0] += 1
                else:
                    #
                    # line_item_metrics (none)
                    #
                    self._li_metrics[li_grp_key][self._li_respmap['none']] += 1
                    self._li_metrics[li_grp_key][0] += 1
        #
        # line_item_metrics (true)
        #
        if events.get("match") is not None:
            tru_liids = events.get("match")
            for tru_liid in tru_liids:
                if (not self._setlineiteminfo(tru_liid)):
                    continue
                li_grp_key = (tru_liid, demo_id)
                if li_grp_key not in self._li_metrics:
                    self._li_metrics[li_grp_key] = [0] * len(self._li_cntrmap)
                self._li_metrics[li_grp_key][self._li_respmap['true']] += 1
                self._li_metrics[li_grp_key][0] += 1

        for event in events:
            if event in self._event_set:
                imp_liid = events[event]
                if isinstance(imp_liid, list):
                    imp_liid = imp_liid[0]
                if (not self._setlineiteminfo(imp_liid)):
                    continue
                if (imp_liid not in self._brand_unsafe_labels):
                    unsafe_label_ids = retry_operation(
                        datamodel.get_brand_unsafe_label_ids, [imp_liid],
                        sleep_time=15)
                    self._brand_unsafe_labels[imp_liid] = set(unsafe_label_ids)
                #
                #  labels
                #
                true_labels = set()
                if self._setlineiteminfo(imp_liid) and imp.get('true_label_ids', []) is not None:
                    true_labels = set(imp.get('true_label_ids', []))
                for label_id in true_labels:
                    lab_groupkey = (imp_liid, label_id, demo_id)
                    if lab_groupkey not in self._labels:
                        self._labels[lab_groupkey] = [0] * len(self._lb_cntrmap)
                    self._labels[lab_groupkey][self._lb_cntrmap[event]] += 1
                    #
                    #  redis
                    #
                    red_groupkey = (imp_liid, label_id, media_type)
                    if video_id is not None and video_id != 0 and video_id != 'None':
                        self._redis_info[red_groupkey].add(video_id)
                    elif app_id is not None and app_id != 0:
                        self._redis_info[red_groupkey].add(app_id)
                    elif page_id is not None and page_id != 0:
                        self._redis_info[red_groupkey].add(page_id)
                #
                #  served_pages
                #
                if page_id != 0:
                    sp_groupkey = (imp_liid, page_id, media_type)
                elif app_id != 0:
                    sp_groupkey = (imp_liid, app_id, media_type)
                else:
                    sp_groupkey = None
                if (sp_groupkey is not None):
                    if (sp_groupkey not in self._served_media):
                        if page_id != 0:
                            if (page_id not in self._pageinfo):
                                self._pageinfo[page_id] = [domain, 0]
                            self._pageinfo[page_id][1] = video_id
                        self._served_media[sp_groupkey] = [0] * len(self._sp_cntrmap)
                    self._served_media[sp_groupkey][self._sp_cntrmap[event]] += 1
                    srv_media = self._served_media[sp_groupkey]
                    if event == 'view':
                        if self._VIEWABILITY_LABEL_ID in true_labels:
                            srv_media[self._sp_cntrmap['viewableimp']] += 1
                        if not (self._brand_unsafe_labels.get(imp_liid) in true_labels):
                            srv_media[self._sp_cntrmap['brandsafe']] += 1
                        srv_media[self._sp_cntrmap['knownimp']] += 1
                #
                # line_item_metrics (events)
                #
                li_grp_key = (imp_liid, demo_id)
                if li_grp_key not in self._li_metrics:
                    self._li_metrics[li_grp_key] = [0] * len(self._li_cntrmap)
                self._li_metrics[li_grp_key][self._li_cntrmap[event]] += 1
                li_metrics = self._li_metrics[li_grp_key]
                if event == 'view':
                    if self._VIEWABILITY_LABEL_ID in true_labels:
                        li_metrics[self._li_cntrmap['viewableimp']] += 1
                    if not (self._brand_unsafe_labels.get(imp_liid) in true_labels):
                        li_metrics[self._li_cntrmap['brandsafe']] += 1
                    li_metrics[self._li_cntrmap['knownimp']] += 1
                #
                #  user counts
                #
                if (isinstance(imp.get("vcpdid"), int)):
                    ugroup = (imp.get('vcpdid'), imp_liid)
                    if (ugroup not in self._userbyli):
                        self._userbyli[ugroup] = [0] * 2
                    if event == 'view':
                        self._userbyli[ugroup][0] += 1
                    if event == 'click':
                        self._userbyli[ugroup][1] += 1
        self._wrlock.release()
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
        tdump = self._etime
        tdump += timedelta(minutes=5) - timedelta(hours=1)
        tdump = snaphour(now=tdump)
        tdbstr = tdump.strftime("%Y-%m-%d %H:%M:%S")
        (served_media, blocked_media, userbyli,
            labels, redisinfo, li_metrics) =\
            self._counters_swap()
        self._log.info("Dump table sizes: served_media[{0}] block_media[{1}] "
                       "users[{2}] labels[{3}] line_item_metrics[{4}] "
                       "redis[{5}]]".format(len(served_media),
                                            len(blocked_media),
                                            len(userbyli),
                                            len(labels),
                                            len(li_metrics),
                                            len(redisinfo)))
        self._dumplock.acquire()
        opath = EventRollup.output_filename(self._TABLE_NAMES[0], tdump,
                                            self._shardindex)
        #
        #  served_media
        #
        with open(opath, "w") as fout:
            for ((liid, media_id, media_type), cntr) in served_media.items():
                if (liid not in self._lineiteminfo):
                    continue
                (campaign_id, adv_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                (domain, video_id) = self._pageinfo.get(media_id,
                                                        (None, None))
                fout.write(
                    "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\n".format(
                        tdbstr, liid, campaign_id, adv_id, publisher_id,
                        (media_type is 'video' or media_type is 'page') and media_id or self._MYSQL_NULL,
                        (media_type is 'video') and video_id or media_id,
                        (domain is not None) and domain or self._MYSQL_NULL,
                        "\t".join(map(str, cntr)),
                        media_type))
        self._dumplock.release()
        #
        #  blocked_media
        #
        self._dumplock.acquire()
        opath = EventRollup.output_filename(self._TABLE_NAMES[1], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for (liid, media_id, media_type) in blocked_media:
                if (liid not in self._lineiteminfo):
                    continue
                (campaign_id, adv_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                (domain, video_id) = self._pageinfo.get(media_id, (None, None))
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\n".format(
                    tdbstr, liid, campaign_id, adv_id, publisher_id,
                    (media_type is 'video' or media_type is 'page') and media_id or self._MYSQL_NULL,
                    (media_type is 'video') and video_id or media_id,
                    (domain, self._MYSQL_NULL)[domain is None],
                    media_type))
        self._dumplock.release()
        #
        #  users
        #
        self._dumplock.acquire()
        opath = EventRollup.output_filename(self._TABLE_NAMES[2], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((user_id, liid), (views, clicks)) in userbyli.items():
                if (liid not in self._lineiteminfo):
                    continue
                (campaign_id, adv_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format(
                           tdbstr, liid, campaign_id, adv_id, publisher_id,
                           user_id, views, clicks))
        self._dumplock.release()
        #
        #  labels
        #
        self._dumplock.acquire()
        opath = EventRollup.output_filename(self._TABLE_NAMES[3], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((liid, label_id, demo_id), cntrs) in labels.items():
                if (liid not in self._lineiteminfo):
                    continue
                (campaign_id, advertiser_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                is_personality = int((label_id in self._PERS_LABELS))
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t"
                           "{7}\t{8}\t{9}\n".format(label_id, advertiser_id,
                                                    campaign_id, publisher_id,
                                                    liid, tdbstr, 0,
                                                    is_personality,
                                                    '\t'.join(map(str, cntrs)),
                                                    (demo_id,
                                                     self._MYSQL_NULL)[demo_id == 0]))
        self._dumplock.release()
        #
        #  line item metrics
        #
        self._dumplock.acquire()
        opath = EventRollup.output_filename(self._TABLE_NAMES[4], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((liid, demo_id), cntrs) in li_metrics.items():
                if liid not in self._lineiteminfo:
                    continue
                (campaign_id, advertiser_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n".format(
                    tdbstr, liid, campaign_id, publisher_id, advertiser_id,
                    '\t'.join(map(str, cntrs)),
                    (demo_id,
                     self._MYSQL_NULL)[demo_id == 0]))
        self._dumplock.release()
        #
        #  redis
        #
        self._dumplock.acquire()
        redis_insert = {}
        opath = EventRollup.output_filename(self._TABLE_NAMES[5], tdump,
                                            self._shardindex)
        with open(opath, "w") as fout:
            for ((liid, label_id, media_type), media_ids) in redisinfo.items():
                if liid not in self._lineiteminfo:
                    continue
                (campaign_id, advertiser_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                # advertise id, label_id
                redis_key = ("A", advertiser_id, label_id, media_type)
                if redis_key not in redis_insert:
                    redis_insert[redis_key] = set()
                redis_insert[redis_key] =\
                    redis_insert[redis_key] | set(media_ids)
                # campaign id, label_id
                redis_key = ("C", campaign_id, label_id, media_type)
                if redis_key not in redis_insert:
                    redis_insert[redis_key] = set()
                redis_insert[redis_key] =\
                    redis_insert[redis_key] | set(media_ids)
                # line item id, label_id
                redis_insert[("L", liid, label_id, media_type)] = set(media_ids)
            for ((key1, key2, key3, key4), values) in redis_insert.items():
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(key1, key2,
                                                              key3, key4,
                           '\t'.join(map(str, values))))
        self._dumplock.release()
        #
        #
        self._metadata_reset()


class EventRollupMgr(MPKinesisMgr):

    def __init__(self, processname, *args, **kwargs):
        super(EventRollupMgr, self).__init__(processname, *args, **kwargs)

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
        self._worker = EventRollup(logger=self._log,
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
    t = EventRollupMgr("event_rollup")
    t.start()
    sys.exit(0)
