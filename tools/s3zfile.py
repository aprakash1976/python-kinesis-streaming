import boto
import zlib
import time
import logging

s3_log = logging.getLogger('s3file')


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

    
if __name__ == "__main__":
    import sys
    import boto

    logging.basicConfig()
    s3_log.setLevel(logging.INFO)
    s3bucket = "affine-hadoop-input"
    s3keyname = "ads_servers/ads-434d7263/events/events.log.2014-03-19_19_45.gz"
    s3c = boto.connect_s3()
    buck = s3c.get_bucket(s3bucket)
    s3key = buck.get_key(s3keyname)
    if (not s3key):
        s3_log.error("cannot find S3 key %s in %s bucket" % (s3keyname, s3bucket))
        sys.exit(1)
    s3zf = S3ZipFile(s3key)
    cnt = 0
    for line in s3zf.next_line():
        cnt += 1
        if (cnt < 10):
            print line.strip('\n')

    print 'Read ', cnt, ' lines'
    
    

