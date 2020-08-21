import asyncio
import logging
import time
import sys
from datetime import datetime, timedelta
from streaming.util.s3evtmsgs import S3EvtmsgCombinedIterator
from streaming import msgmodel
from streaming.einterposer import Bundler, cd_add_impid, TSSeq


last_ts = None
last_seqnum = 0
def get_next_tseq():
    global last_ts, last_seqnum
    it0 = int(time.time())
    if (it0 != last_ts):
        last_ts = it0
        last_seqnum = 0
    last_seqnum += 1
    return TSSeq.fromstring("{0:d}.{1:x}".format(last_ts, last_seqnum))

#
#   basic script to inject raw S3 events into a Kinesis raw stream
#
#
loop   = asyncio.get_event_loop()
logger = logging.getLogger("__main__")
tstart = datetime(2015,3,1,17,50)
tend   = tstart + timedelta(hours=2)
print tstart, ': ', tend
s3iter = S3EvtmsgCombinedIterator(tstart, tend)
ss = s3iter.next()
for rec in ss:
    print rec['timestamp']
print rcnt

sys.exit(0)


rcnt = 0
b = Bundler("AdsQueryRaw.dev", setpartkey=(lambda r: r['ii'].split("_")[0]), efilter=(lambda r: 'ii' in r), emanip=cd_add_impid, logger=logger,
            loop = loop)
seqnum = 0
for rec in s3iter:
    if ('ii' in rec):
        ii = rec['ii'].split("_")[0]
        iz = int(ii[-5:], 16)
        if ((iz % 10) == 0):
            rcnt += 1
            b.add(dict(seq=get_next_tseq(), record=rec))
print rcnt


