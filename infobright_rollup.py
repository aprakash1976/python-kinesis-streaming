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
from hashlib import sha1
from affine import normalize_url
from affine.retries import retry_operation
from streaming.util import shardid2index, snaphour,\
    args_parser, TCounter
from streaming.kinesisbolt import KinesisSpoutUnbundler
from streaming.bolt import BoltBase
from affine.log import configure_logging
from affine.model import LineItem, Campaign, session, Label
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


class InfobrightRollup(BoltBase):
    _REASON_UNKNOWN_URL = 1
    _VIEWABILITY_LABEL_ID = 7840
    _MYSQL_NULL = "\\N"
    _BLOCKED_MEDIA_MAX_ROWS = 10
    _DOMAIN_MAXLEN = 128
    _DUMP_VERSION = 2
    _IAB_LABELS = (1041, 1051, 2136, 2161, 2184, 2215, 2325, 2234, 1055, 2422,
                   2441, 2452, 2459, 2520, 2758, 2774, 2765, 2484, 1057, 2619,
                   2632, 2707)
    _TABLE_NAMES = (
        "served_media",
        "blocked_media",
        "users",
        "true_domain_reasons",
        "false_domain_reasons",
        "bid_requests_page",
        "labels",
        "line_item_metrics",
        "redis",
        "queue_loader",
        "bid_requests_app"
    )

    def __init__(self, *args, **kwargs):
        super(InfobrightRollup, self).__init__(*args, **kwargs)
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
        self.load_personality_labels()
        self._served_media = None
        self._li_metrics = None
        self._blocked_media = None
        self._userbyli = None
        self._page_req = None
        self._domain_reasons = None
        self._labels = None
        self._redis_info = None
        self._queue_loader = {}
        self._noncontext_liids = {}
        self._counters_swap()
        self._metadata_reset()
        self._log.info("Infobright init complete")
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
        page_req = self._page_req
        domain_reasons = self._domain_reasons
        labels = self._labels
        redisinfo = self._redis_info
        queueloader = self._queue_loader
        non_contx_liids = self._noncontext_liids
        li_metrics = self._li_metrics
        self._served_media = {}
        self._blocked_media = set()
        self._li_metrics = {}
        self._userbyli = {}
        self._page_req = Counter()
        self._domain_reasons = Counter()
        self._labels = {}
        self._falselimit = Counter()
        self._redis_info = defaultdict(set)
        self._queue_loader = {}
        self._noncontext_liids = {}
        self._wrlock.release()
        return (served_media, blocked_media, userbyli, page_req,
                domain_reasons, labels, redisinfo, queueloader,
                non_contx_liids, li_metrics)

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
        if self._current_time != imp.get('timestamp'):
            self._current_time = imp.get('timestamp')
            self._etime = datetime.strptime(self._current_time,
                                            '%Y-%m-%d %H:%M:%S')
            if (self._first_ts is None):
                self._first_ts = self._etime
                self._last_checkpt = snap_minute(self._etime,
                                                 self._checkpoint_interval
                                                 )
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
        #
        #  event procesing/rollups
        #
        if 'url' in imp:
            domain = self._fast_getdomain(imp['url'])
            if (domain is None):
                domain = normalize_url.domain_of_url(imp['url'])
        if (domain is not None):
            domain = domain[:self._DOMAIN_MAXLEN]
        page_id = imp.get('page_id', 0)
        app_id = imp.get('app_id', 0)
        video_id = imp.get('video_id', 0)
        country_code = imp.get('country_code', 'US')
        demo_id = imp.get('demo_id', 0)
        if demo_id is None:
            demo_id = 0
        if app_id > 0:
            media_type = 'app'
        elif video_id > 0:
            media_type = 'video'
        else:
            media_type = 'page'
        self._wrlock.acquire()
        #
        #  accumulate event counters
        #
        if (page_id or app_id):
            if page_id > 0:
                page_grpkey = (page_id, country_code, media_type)
            elif app_id and app_id > 0:
                page_grpkey = (app_id, country_code, media_type)
            self._page_req[page_grpkey] += 1
        else:
            self._cntr.incr("nopage")
        if imp.get('imp_line_item_id') and (page_id != 0 or app_id != 0):
            liid = int(imp['imp_line_item_id'])
            if (not self._setlineiteminfo(liid)):
                self._wrlock.release()
                return (False, None)

            if (liid not in self._brand_unsafe_labels):
                unsafe_label_ids = retry_operation(
                    datamodel.get_brand_unsafe_label_ids, [liid],
                    sleep_time=15)
                self._brand_unsafe_labels[liid] = set(unsafe_label_ids)
            elist = imp['event_list']
            #
            #  served pages
            #
            if page_id != 0:
                groupkey = (liid, page_id, media_type)
            else:
                groupkey = (liid, app_id, media_type)
            incr = self._evt2incr(elist, self._sp_cntrmap)
            if (groupkey not in self._served_media):
                if page_id != 0:
                    if (page_id not in self._pageinfo):
                        self._pageinfo[page_id] = [domain, 0]
                    self._pageinfo[page_id][1] = video_id
                self._served_media[groupkey] = [0] * len(self._sp_cntrmap)
            self._served_media[groupkey] = map(sum, zip(
                self._served_media[groupkey], incr))

            has_view_event = any(map((lambda x: x[0] == 'view'), elist))
            srv_media = self._served_media[groupkey]
            if has_view_event:
                if self._VIEWABILITY_LABEL_ID in imp.get('true_label_ids', []):
                    srv_media[self._sp_cntrmap['viewableimp']] += 1
                if not (self._brand_unsafe_labels[liid] in
                        imp.get('true_label_ids', [])):
                    srv_media[self._sp_cntrmap['brandsafe']] += 1
                srv_media[self._sp_cntrmap['knownimp']] += 1
            #
            #  user counts
            #
            if (isinstance(imp.get("user_id"), int)):
                ugroup = (imp['user_id'], liid)
                if (ugroup not in self._userbyli):
                    self._userbyli[ugroup] = [0] * 2
                self._userbyli[ugroup] = map(sum, zip(self._userbyli[ugroup],
                                                      incr[:2]))
            #
            #  labels
            #
            incr = self._evt2incr(elist, self._lb_cntrmap)
            for label_id in imp.get('true_label_ids', []):
                groupkey = (liid, label_id, demo_id)
                if groupkey not in self._labels:
                    self._labels[groupkey] = [0] * len(self._lb_cntrmap)
                self._labels[groupkey] = map(sum, zip(self._labels[groupkey],
                                                      incr))
                #
                #  redis
                #
                rgroupkey = (liid, label_id, media_type)
                if video_id is not None and video_id != 0 and video_id !=\
                   'None':
                    self._redis_info[rgroupkey].add(video_id)
                elif app_id is not None and app_id != 0:
                    self._redis_info[rgroupkey].add(app_id)
                else:
                    self._redis_info[rgroupkey].add(page_id)
            #
            #  line item metrics
            #
            incr = self._evt2incr(elist, self._li_cntrmap)
            li_grp_key = (liid, demo_id)
            if li_grp_key not in self._li_metrics:
                self._li_metrics[li_grp_key] = [0] * len(self._li_cntrmap)
            self._li_metrics[li_grp_key] = map(sum, zip(self._li_metrics[li_grp_key], incr))
            if has_view_event:
                if self._VIEWABILITY_LABEL_ID in imp.get('true_label_ids', []):
                    self._li_metrics[li_grp_key][self._li_cntrmap['viewableimp']] += 1
                if not (self._brand_unsafe_labels[liid] in
                        imp.get('true_label_ids', [])):
                    self._li_metrics[li_grp_key][self._li_cntrmap['brandsafe']] += 1
                self._li_metrics[li_grp_key][self._li_cntrmap['knownimp']] += 1

        #   non-contextual
        if imp.get('imp_line_item_id') and \
           page_id == 0 and \
           ('app_id' not in imp):
            liid = imp.get('imp_line_item_id')
            found = self._setlineiteminfo(liid)
            if found:
                campaign_id, adv_id, publisher_id, is_contextual =\
                    self._lineiteminfo[liid]
                elist = imp['event_list']
                incr = self._evt2incr(elist, self._li_cntrmap)
                if is_contextual is False:
                    self._log.debug('added to noncontext')
                    li_grp_key = liid, demo_id
                    if li_grp_key not in self._li_metrics:
                        self._li_metrics[li_grp_key] = [0] * len(
                            self._li_cntrmap)
                    self._li_metrics[li_grp_key] = map(sum, zip(
                        self._li_metrics[li_grp_key], incr))
                    self._log.debug(elist)
                    self._log.debug(liid)
                    self._log.debug(self._li_metrics[li_grp_key])
        #
        #  accumulate query responses (grouped-by domain)
        #
        if ('line_item_responses' in imp):
            line_item_responses = imp['line_item_responses']
            queue_loader_added = False
            for liid, resptype, reason_code, reason_details in\
                    line_item_responses:
                found = self._setlineiteminfo(liid)
                if (not found):
                    continue
                response_type = self._respmap[resptype]
                # queue loader
                if (queue_loader_added is False and response_type == 'none' and
                        reason_code == self._REASON_UNKNOWN_URL):
                    key = (sha1(imp['url']).hexdigest(), country_code)
                    if key not in self._queue_loader:
                        self._queue_loader[key] = {'url': imp['url'],
                                                   'domain': domain,
                                                   'count': 1}
                        queue_loader_added = True
                    else:
                        self._queue_loader[key]['count'] += 1
                        queue_loader_added = True
                if (response_type == 'false') and (page_id != 0 or
                                                   app_id != 0):
                    falsegroup = (reason_code, reason_details)
                    self._falselimit[falsegroup] += 1
                    if (self._falselimit[falsegroup] <=
                            self._BLOCKED_MEDIA_MAX_ROWS):
                        if page_id != 0:
                            if (page_id not in self._pageinfo):
                                self._pageinfo[page_id] = [domain, 0]
                            self._pageinfo[page_id][1] = video_id
                            self._blocked_media.add((liid, page_id,
                                                     media_type))
                        elif app_id != 0:
                            self._blocked_media.add((liid, app_id, media_type))

                if (response_type != 'none'):
                    groupkey = (response_type, liid, reason_code,
                                reason_details)
                    self._domain_reasons[groupkey] += 1
                li_grp_key = (liid, demo_id)
                if li_grp_key not in self._li_metrics:
                    self._li_metrics[li_grp_key] = [0] * len(self._li_cntrmap)
                self._li_metrics[li_grp_key][self._li_respmap[response_type]] += 1
                self._li_metrics[li_grp_key][0] += 1

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
        (served_media, blocked_media, userbyli, page_req, domain_reasons,
            labels, redisinfo, queueloader, non_contx_liids, li_metrics) =\
            self._counters_swap()
        self._log.info("Dump table sizes: served_media[{0}] block_media[{1}] "
                       "users[{2}] domain_reasons[{3}] bid_requests[{4}] "
                       "labels[{5}] line_item_metrics[{6}] redis[{7}] "
                       "queueloader[{8}] nonctx[{9}]".format(len(served_media),
                                                             len(blocked_media),
                                                             len(userbyli),
                                                             len(domain_reasons),
                                                             len(page_req),
                                                             len(labels),
                                                             len(li_metrics),
                                                             len(redisinfo),
                                                             len(queueloader),
                                                             len(non_contx_liids)))
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[0], tdump,
                                                 self._shardindex)
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
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[1], tdump,
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
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[2], tdump,
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
        #  true_domain_reasons / false_domain_reasons
        #
        self._dumplock.acquire()
        for table_name in ('true_domain_reasons', 'false_domain_reasons'):
            tresponse = table_name.replace("_domain_reasons", "")
            opath = InfobrightRollup.output_filename(
                table_name, tdump, self._shardindex)
            with open(opath, "w") as fout:
                for ((response_type, liid, reason_code, reason_details),
                     qcount) in domain_reasons.items():
                    if (liid not in self._lineiteminfo):
                        continue
                    if (response_type == tresponse):
                        (campaign_id, adv_id, publisher_id, is_contextual) =\
                            self._lineiteminfo[liid]
                        fout.write(
                            "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\n".format(
                                tdbstr, liid, campaign_id, adv_id,
                                self._MYSQL_NULL,
                                reason_code,
                                (reason_details,
                                 self._MYSQL_NULL)[reason_details is None],
                                qcount))
        self._dumplock.release()
        #
        #  bid_requests (page/video)
        #
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[5], tdump,
                                                 self._shardindex)
        with open(opath, "w") as fout:
            for ((page_id, country_code, apporurl), count) in page_req.items():
                if apporurl is 'page' or apporurl is 'video':
                    fout.write("{0}\t{1}\t{2}\t{3}\n".format(tdbstr,
                                                             page_id,
                                                             count,
                                                             country_code))
        self._dumplock.release()
        #
        #  labels
        #
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[6], tdump,
                                                 self._shardindex)
        with open(opath, "w") as fout:
            for ((liid, label_id, demo_id), cntrs) in labels.items():
                if (liid not in self._lineiteminfo):
                    continue
                (campaign_id, advertiser_id, publisher_id, is_contextual) =\
                    self._lineiteminfo[liid]
                is_iab = int((label_id in self._IAB_LABELS))
                is_personality = int((label_id in self._PERS_LABELS))
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t"
                           "{7}\t{8}\t{9}\n".format(label_id, advertiser_id,
                                                    campaign_id, publisher_id,
                                                    liid, tdbstr, is_iab,
                                                    is_personality,
                                                    '\t'.join(map(str, cntrs)),
                                                    (demo_id,
                                                     self._MYSQL_NULL)[demo_id == 0]))
        self._dumplock.release()
        #
        #  line item metrics
        #
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[7], tdump,
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
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[8], tdump,
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
        #  queue-loader
        #
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[9], tdump,
                                                 self._shardindex)
        with open(opath, "w") as fout:
            for ((shard_key, country_code), dict_values) in queueloader.items():
                url = dict_values['url']
                domain = dict_values['domain']
                count = dict_values['count']
                try:
                    subdomain = normalize_url.domain_of_url(url,
                                                            with_subdomains=True)
                except:
                    self._log.warn("Skipped unparseable URL: {0}".format(url))
                    continue
                fout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(shard_key, url,
                           domain, subdomain, country_code, count))
        self._dumplock.release()
        #
        #  bid_requests (apps)
        #
        self._dumplock.acquire()
        opath = InfobrightRollup.output_filename(self._TABLE_NAMES[10], tdump,
                                                 self._shardindex)
        with open(opath, "w") as fout:
            for ((page_id, country_code, apporurl), count) in page_req.items():
                if apporurl is 'app':
                    fout.write("{0}\t{1}\t{2}\t{3}\n".format(tdbstr,
                                                             page_id,
                                                             count,
                                                             country_code))
        self._dumplock.release()
        #
        #
        self._metadata_reset()


class InfobrightRollupMgr(MPKinesisMgr):

    def __init__(self, processname, *args, **kwargs):
        super(InfobrightRollupMgr, self).__init__(processname, *args, **kwargs)

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
        self._worker = InfobrightRollup(logger=self._log,
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
    t = InfobrightRollupMgr("infobright_rollup")
    t.start()
    sys.exit(0)
