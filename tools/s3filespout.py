from streaming.bolt import BoltBase, SpoutBase
from datetime import datetime
import zlib
import time
import logging

s3_log = logging.getLogger('s3file')


class S3FileSpout(SpoutBase):
    _event_field_names = (
        "timestamp_str",        "line_item_id",        "request_id",
        "channel_id",           "channel_name",        "media_partner_name",
        "url",                  "impression_id",       "event_type",
        "response",             "user_id",             "reason_code",
        "reason_details",       "entry_type",          "video_url",
        "adserver_id",          "page_id",             "video_id",
        "player_width",         "player_height",       "pub_id"
    )

    def _str_decompress(self, stream):
        for chunk in stream:
            rv = self._decomp.decompress(chunk)
            if (rv):
                yield rv


    def _get_next_line(self, stream):
        rem = ""
        for buf in self._str_decompress(stream):
            buf = rem + buf
            while (len(buf)>0):
                idx = buf.find('\n')
                if (idx < 0):
                    rem = buf[:]
                    break
                line = buf[:idx+1]
                buf  = buf[idx+1:]
                yield line

    @classmethod
    def tsv2evt(cls, tsv):
        d = dict(zip(S3FileSpout._event_field_names, tsv))
        e = { k:v for (k,v) in d.iteritems() if (len(v) > 0) }
        return e

    def __init__(self, *args, **kwargs):
        super(S3FileSpout, self).__init__(*args, **kwargs)
        self._s3keys = args
        self._inf = None
        self._lcount = 0
        self._t0 = time.time()

    def _get_next_event(self):
        for s3key in self._s3keys:
            s3_log.info("opening s3 file: %s" % s3key)
            t00 = None
            self._decomp = zlib.decompressobj(16+zlib.MAX_WBITS)
            for xl in self._get_next_line(s3key):
                f = xl.strip().split('\t')
                imp_id = f[7]
                if (len(imp_id) == 0): continue
                e = self.tsv2evt(f)
                if ((self._lcount % 50000) == 0):
                    tnow = datetime.strptime(e["timestamp_str"], "%Y-%m-%d %H:%M:%S")
                    if (t00 is None): t00 = tnow
                    s3_log.warn("line: % 10d  eventelap: %ds   elap: %.1fs   %s" % (self._lcount, (tnow-t00).total_seconds(), (time.time() - self._t0), imp_id))
#                if (self._lcount > 10000): break
                self._lcount += 1
                yield e
            
    
if __name__ == "__main__":
    import sys
    import boto


    class DebugBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(DebugBolt, self).__init__(*args, **kwargs)
            self._pcount = 0
        def process(self, data):
            if (self._pcount < 1000):
                self._pcount += 1
                s3_log.info("I: %s" % data['impression_id'])
            return None

    logging.basicConfig()
    s3_log.setLevel(logging.INFO)
    s3bucket = "affine-hadoop-input"
    s3keyname = "ads_servers/ads-434d7263/events/events.log.2014-03-06_19_45.gz"
    s3c = boto.connect_s3()
    buck = s3c.get_bucket(s3bucket)
    s3key = buck.get_key(s3keyname)
    if (not s3key):
        s3_log.error("cannot find S3 key %s in %s bucket" % (s3keyname, s3bucket))
        sys.exit(1)
    f = S3FileSpout(s3key)
    q = DebugBolt(label="fake")
    f.addsink(q)
    s3_log.info("starting S3file spout")
    f.run()

    
    

