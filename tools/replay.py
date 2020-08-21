from datetime import datetime, timedelta
import boto
import zlib
import time
import re
import Queue
import requests
import urllib2
import urlparse
import logging
import collections
import concurrent.futures
import random
import sys
import json
import io

S3_MOD_STRFRMT = '%Y-%m-%dT%H:%M:%S.000Z'
url_regex = re.compile(r'^[\d\.]+[\s\-]+\[.+\] \"GET (\/(query_line_item|(?P<reg>event\/register)|(?P<lb>labels))\S+).+$')
sub_imp_regex = re.compile(r'(impression_id\=)[a-z0-9\_]+')
#
#  Sampled recent impression IDs returned from query service
#  Used to manufacture views/complete etc events with valid impression IDs
#
query_impressions = collections.deque()


def event_writer(outfile, evtQ):
    t0 = time.time()
    with io.open(outfile, mode="wt", buffering=1) as fout:
        while (True):
            iie = evtQ.get()
            if (iie is None):
                break
            fout.write(u"{0}\t{1}\n".format(iie[0], iie[1]))
            evtQ.task_done()
        evtQ.task_done()


class S3ZipFile(object):
    def _str_decompress(self):
        for chunk in self._stream:
            rv = self._decomp.decompress(chunk)
            if (rv):
                yield rv

    def next_line(self):
        rem = ""
        for buf in self._str_decompress():
            buf = rem + buf
            while (len(buf)>0):
                idx = buf.find('\n')
                if (idx < 0):
                    rem = buf[:]
                    break
                line = buf[:idx+1]
                buf  = buf[idx+1:]
                yield line



    def __init__(self, s3stream, **kwargs):
        self._stream = s3stream
        self._decomp = zlib.decompressobj(16+zlib.MAX_WBITS)



def get_ads_running():
    ads_running= [ "dc6-ads-{0}".format(x) for x in xrange(1,31) ]
    return ads_running


def s3names_getbytimerange(bucket, server_name, start, end):
    names = []
    S3_KEY = "ads_servers/%s/access" % server_name
    for key in list(bucket.list(prefix=S3_KEY)):
        file_m_time = datetime.strptime(key.last_modified, S3_MOD_STRFRMT)
        if (file_m_time >= start) and (file_m_time <= end):
            names.append(key.name)
    return names


def name2gen(bucket, s3name):
    skey = bucket.get_key(s3name)
    s3o  = S3ZipFile(skey)
    sgen = s3o.next_line()
    return (skey, s3o, sgen)


def get_next_url(bucket, s3names, recent_true_iid, limit):
    ucount = 0
    for adhost in s3names.keys():
        if (len(s3names[adhost])):
            sname = s3names[adhost].pop(0)
            (skey, s3rawfile, s3gen) = name2gen(bucket, sname)
            for line in s3gen:
                mo = url_regex.match(line.strip())
                if (not mo): continue
                url = mo.group(1)
                if (mo.group('reg') and len(recent_true_iid)):
                    ii = recent_true_iid.popleft() 
                    url =  sub_imp_regex.sub('\g<1>'+ii, url)
                elif mo.group('lb'):
                    pass
                ucount += 1
                if (limit is not None) and (ucount > limit):
                    raise StopIteration
                yield url


def bulk_url_fetch(uris):
    #  tried using requests module but it was twice as slow (?)
    evturirex = re.compile(r'^.+event\/register.+impression_id=([0-9a-f_]+).*event_type=([a-z]+).*$')
    responses = []
    for uri in uris:
        try:
            u = urllib2.urlopen(uri)
        except:
            continue
        if (u) and (u.getcode() == 200):
            content_type = u.info().gettype()
            if (content_type == 'application/json'):
                g = u.read()
                try:
                    ev = json.loads(g)
                except:
                    continue
                responses.append(ev)
            elif (content_type == 'image/gif'):
                mo = evturirex.match(uri)
                if (mo):
                    responses.append(dict(impression_id=mo.group(1), event_type=mo.group(2)))
            u.close()
    return responses



class Replayer(object):
    _DEFAULT_SERVER_COUNT = 2
    _DEFAULT_BUCKET = "affine-hadoop-input"
    def __init__(self, host, **kwargs):
        self._host = host
        self._server_count = kwargs.get('server_count', self._DEFAULT_SERVER_COUNT)
        self._bucket_name = kwargs.get('bucket_name', self._DEFAULT_BUCKET)
        self._maxconcur = kwargs.get('concur', 5)
        self._batchsize = kwargs.get('batchsize', 100)
        self._tend = kwargs.get('tend', datetime.utcnow() - timedelta(days=1))
        self._tstart = (kwargs.get('tstart', self._tend) < self._tend) and \
            kwargs['tstart'] or (self._tend - timedelta(hours=4))
        self._quiet = kwargs.get('quiet', False)
        self._setup_s3event_source()

    def _setup_s3event_source(self):
        adservs = get_ads_running() 
        adservs = random.sample(adservs, self._server_count)
        s3_conn = boto.connect_s3()
        bucket = s3_conn.get_bucket(self._bucket_name)
        self._s3names = {}
        for adhost in adservs:
            s3n = s3names_getbytimerange(bucket, adhost, self._tstart, self._tend)
            if (len(s3n) > 0):
                self._s3names[adhost] = s3n
        s3_conn.close()

    def runner(self, duration, evtQ=None, limit=None):
        tstart = time.time()
        tprint = tstart
        ucount = 0
        for resp in self.post_query(limit):
            tnow = time.time()
            telap = tnow - tstart
            if (telap > duration):
                break
            ucount += 1
            if ('event_type' in resp) and (evtQ) and (not evtQ.full()):
                try:
                    evtQ.put((resp['impression_id'], resp['event_type']), False)
                except Queue.Full:
                    pass
            if (not self._quiet) and ((tnow - tprint)>5.0):
                rate = float(ucount)/(tnow - tprint)
                print 'Elapsed @ {0:7.1f}s   {1:7.1f} URL/s'.format(telap, rate)
                ucount = 0
                tprint = tnow

            
    def post_query(self, limit):
        s3_conn = boto.connect_s3()
        bucket = s3_conn.get_bucket(self._bucket_name)
        recent_true_iid = collections.deque([], 300)
        with concurrent.futures.ThreadPoolExecutor(max_workers=(self._maxconcur+1)) as executor:
            running = []
            uris = []
            for upath in get_next_url(bucket, self._s3names, recent_true_iid, limit):
                uris.append("http://" + self._host + upath)
                if (len(uris) == self._batchsize):
                    uris2 = uris
                    random.shuffle(uris2)
                    uris = []
                    if (len(running) >= self._maxconcur):
                        (x,y) = concurrent.futures.wait(running, return_when=concurrent.futures.FIRST_COMPLETED)
                        running = list(y)
                        finished = list(x)
                        newimps = []
                        for ftdone in finished:
                            for response in ftdone.result():
                                if (isinstance(response, dict)):
                                    if (response.get('line_items')):
                                        recent_true_iid.append(response['line_items'].values()[0])
                                    yield response
                    running.append(executor.submit(bulk_url_fetch, uris2))
            running.append(executor.submit(bulk_url_fetch, uris2))
            (x,y) = concurrent.futures.wait(running, return_when=concurrent.futures.ALL_COMPLETED)


if __name__ == "__main__":
    #
    #  cmd-line args
    #
    cmd_line_setters = {
        'D' : ('dayoffset',     int,                  1),
        'd' : ('duration',      (lambda x: x),     "1h"),
        'b' : ('batchsize',     int,                100),
        'S' : ('servers',       int,               None),
        'C' : ('concur',        int,                  5),
        }
    opts = collections.defaultdict(bool)
    for cl,cfg in cmd_line_setters.items():
        if (len(cfg)>2) and (cfg[2] is not None):
            opts[cfg[0]] = cfg[2]

    argv = sys.argv[1:]
    while (len(argv)):
        arg = argv.pop(0)
        if arg.startswith('-'):
            if (arg[1] in cmd_line_setters.keys()):
                (label, transform, default) = cmd_line_setters[arg[1]]
                if (transform is not None):
                    val = argv.pop(0)
                    opts[label] = transform(val)
                else:
                    opts[label] = True
            else:
                sys.stderr.write("Unrecognized option: %s\nUsage: %s -[%s] <stream_name>\n" % (arg, __file__, ''.join(cmd_line_setters.keys())))
                sys.exit(1)
        else:
            opts['target'] = arg

    print opts
    dconv = {'d': (3600*24), 'h': 3600, 'm': 60, 's': 1}
    unit = opts['duration'][-1]
    if (unit not in dconv):
        sys.stderr.write("Unrecognized time unit: %s\n" % opts['duration'])
        sys.exit(1)
    durations = int(opts['duration'][:-1])*dconv[unit]

    base_t = datetime.utcnow()
    opts['tend']   = base_t - timedelta(days=opts['dayoffset'])
    opts['tstart'] = opts['tend'] - timedelta(hours=4)

    cfuture = logging.getLogger('concurrent.futures')
    cfuture.addHandler(logging.StreamHandler())

    print 'Running for ', durations, ' seconds'
    rep = Replayer(opts['target'], **opts)
    rep.runner(durations)
    sys.exit(0)


