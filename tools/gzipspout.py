from streaming.bolt import BoltBase, SpoutBase
from datetime import datetime
import gzip
import zlib
import time
import logging

gz_log = logging.getLogger('gzipfile')


class GZipSpout(SpoutBase):
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
        d = dict(zip(GZipSpout._event_field_names, tsv))
        e = { k:v for (k,v) in d.iteritems() if (len(v) > 0) }
        return e

    def __init__(self, fname, *args, **kwargs):
        super(GZipSpout, self).__init__(*args, **kwargs)
        self._zfile = fname
        self._inf = None
        self._lcount = 0
        self._t0 = time.time()

    def _get_next_event(self):
        gz_log.info("opening gzip file: %s" % self._zfile)
        zstream = open(self._zfile, "rb")
        t00 = None
        self._decomp = zlib.decompressobj(16+zlib.MAX_WBITS)
        for xl in self._get_next_line(zstream):
            f = xl.strip().split('\t')
            imp_id = f[7]
            if (len(imp_id) == 0): continue
            e = self.tsv2evt(f)
            if ((self._lcount % 50000) == 0):
                tnow = datetime.strptime(e["timestamp_str"], "%Y-%m-%d %H:%M:%S")
                if (t00 is None): t00 = tnow
                gz_log.warn("line: % 10d  eventelap: %ds   elap: %.1fs   %s" % (self._lcount, (tnow-t00).total_seconds(), (time.time() - self._t0), imp_id))
#                if (self._lcount > 10000): break
            self._lcount += 1
            yield e
            
    
if __name__ == "__main__":
    import sys

    class DebugBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(DebugBolt, self).__init__(*args, **kwargs)
            self._pcount = 0

        def process(self, data):
            if (self._pcount < 10000):
                self._pcount += 1
                gz_log.info("I: %s" % data['impression_id'])
            return None

    logging.basicConfig()
    gz_log.setLevel(logging.INFO)
    f = GZipSpout(sys.argv[1])
    q = DebugBolt(label="fake")
    f.addsink(q)
    gz_log.info("starting GZip spout")
    f.run()

    
    

