from datetime import datetime
from energeia import EnergeiaClient
import copy
import time
import uuid
import random
import string
import collections
import msgpack
import socket

url_list = set([])






def impression_from(ts, **kwargs):
    e = dict(timestamp_str=datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
             user_id=kwargs.get('user_id', str(uuid.uuid4())),
             event_type=kwargs.get("event_type", "impression"),
             impression_id=kwargs.get("impression_id", str(uuid.uuid4())),
             request_id=kwargs.get("request_id", str(uuid.uuid4())),
             entry_type="qs-event")
    return e

def random_query(ts, **kwargs):
    line_item_id = random.choice(kwargs.get('lineitems', range(10000,20000)))
    e = dict(timestamp_str=datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
             line_item_id=kwargs.get("line_item_id", line_item_id),
             request_id=kwargs.get("request_id", str(uuid.uuid4())),
             channel_id=kwargs.get("line_item_id", random.randint(100,200)),
             media_partner_name="LiveRail",
             url="http://" + ''.join(random.sample(string.lowercase, 10)) + ".com/" + ''.join(random.sample(string.lowercase, 12)),
             impression_id=kwargs.get("request_id", str(uuid.uuid4())) + '_' + str(line_item_id),
             event_type="query",
             response=kwargs.get("response", random.choice(("True", "False", "None"))),
             entry_type="qs-query")
    if (len(url_list)>0):
        e['url']=random.sample(url_list,1)[0]
        if (kwargs.get('one_time', False) and (len(url_list)>1)):
            url_list.remove(e['url'])
    if (e['response'] != "True"):
        e["reason_code"] = random.randint(1,100)
    return e



class FakeAdservEventSource(object):
    _impression_seq = {'query' : 'impression',
                       'impression' : 'firstQuartile',
                       'firstQuartile': 'midpoint',
                       'midpoint': 'thirdQuartile',
                       'thirdQuartile': 'complete'}
                           
    _response_freq = ["True"] + ["False"] * 2 + ["None"] * 3
    def __init__(self, *args, **kwargs):
        super(FakeAdservEventSource, self).__init__()
        self._tinit = int(time.time())
        self._count = 0
        self._imp = collections.deque()
        self._clock_mult = kwargs.get('multiplier', 1.00)
        self._time_base  = kwargs.get('time_base', time.time())
        self._1time = kwargs.get('1time', False)
        self._lineitems = kwargs.get('lineitems', range(10000,20000))
        self._userids = kwargs.get('userids', None)

    def get_next_event(self):
        self._count += 1
        tdelta = (time.time() - self._tinit) * self._clock_mult
        timestamp = self._time_base + tdelta 
        if ((len(self._imp)>0) and (random.randint(0,1) == 0)):
            evt = self._imp.popleft()
            if (random.randint(0,1) == 0) and (evt["event_type"] != "complete"):
                if ('user_id' not in evt) and (self._userids):
                    evt['user_id'] = random.choice(self._userids)
                newevt = impression_from(timestamp, **evt)
                newevt['event_type'] = self._impression_seq[evt["event_type"]]
                self._imp.append(newevt)
                newevt['sequence'] = self._count
                return newevt
        response = random.choice(self._response_freq)
        query = random_query(timestamp,
                             response=response,
                             lineitems=self._lineitems,
                             one_time=self._1time)
        
        if (response == 'True') and (random.randint(0,1) == 0):
            self._imp.append(query)
        query['sequence'] = self._count
        return query


if __name__ == "__main__":
    import random
    import logging
    import sys
    import gzip
    logging.basicConfig()

    default_event_count = 500
    cmd_line_setters = {
        'c' : ('count', int),
        'r' : ('rate', int),
        'U' : ('urlfile', str),
        'M' : ('multiplier', float),
        'B' : ('time_base', int),
        'u' : ('urls', (lambda x: x.split(','))),
        'L' : ('lineitems', (lambda x: map(int,x.split(',')))),
        '1' : ('1time', None),
        'q' : ('userids', (lambda x: x.split(',')))
        }

    opts = {}
    argv = sys.argv[1:]
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

    if ('lineitems' in opts):
        logging.warn('Explicit lineitems: %s' % ','.join(map(str,opts['lineitems'])))
    if ('userids' in opts):
        logging.warn('Explicit userids: %s' % ','.join(map(str,opts['userids'])))

    if ('urls' in opts):
        url_list = opts['urls'][:]
    elif ('urlfile'in opts):
        pagedataf = gzip.GzipFile(opts['urlfile'], "r")
        for line in pagedataf:
            f = line.strip('\n').split('\t')
            if (len(f) != 6): continue
            url_list.add(f[2])
        pagedataf.close()
        uf = open('urllist.txt', 'w')
        for u in url_list:
            uf.write("%s\n" % u)
        uf.close()

    
    eventsource = FakeAdservEventSource(**opts)

    if (True):
        sock = socket.create_connection(("localhost", 9432))
        packer = msgpack.Packer()
    else:
        kclient = EnergeiaClient()
        streamname = "AdsQueryRaw"
        sinfo = kclient.describe_stream(streamname)
        sinfo = sinfo['StreamDescription']
        print sinfo

    sleep_intvl = 1.0/float(opts.get('rate',100))
    print 'Sleep interval: ', sleep_intvl
    print 'Time base: ', time.ctime(opts.get('time_base', time.time())),  '  Clock Multiplier: ', opts.get('multiplier',1.00)
    cnt = 0
    while (True):
        evt = eventsource.get_next_event()
        if (True):
            sock.send(packer.pack(evt))
            line = sock.recv(1024)
            if (not line): break
        else:
            resp = kclient.put_record(streamname, evt, evt['impression_id'])
        time.sleep(sleep_intvl)
        cnt += 1
        if ('count' in opts) and (cnt >= opts['count']): break



    
