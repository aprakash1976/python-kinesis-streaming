import sys
import collections
import socket
import boto
import sys
import logging
import msgpack
import datetime
import time
from streaming import einterposer
from s3filespout import S3FileSpout
from gzipspout import GZipSpout
from newfilespout import NewFileSpout
from streaming.bolt import BoltBase, SpoutBase
from streaming import msgmodel



class InterposerSink(BoltBase):
    def __init__(self, *args, **kwargs):
        super(InterposerSink, self).__init__(*args, **kwargs)
#        self._loop = kwargs.get("loop", asyncio.get_event_loop)
        self._host = kwargs.get("host", "localhost")
        self._port = kwargs.get("port", 9432)
        self._upath = kwargs.get("unixpath", einterposer._DEFAULT_UNIX_PATH)
        if ('unix' in kwargs) and (kwargs['unix']):
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.connect(self._upath)
        else:
            self._sock = socket.create_connection((self._host, self._port))
        self._packsize = kwargs.get('packsize', 25)
        self._recbuff = []

    def connect(self):
#        (sreader, swriter) = yield asyncio.open_connection(host=self._host, port=self._port, loop=self._loop)
#        self._sreader = sreader
#        self._swriter = swriter
        pass

    def pack(self, record):
        return msgpack.packb(record)

    def process(self, record):
        self._recbuff.append(msgmodel.compact(record))
        if (len(self._recbuff) == self._packsize):
            precord = self.pack(self._recbuff)
#            x = yield self._swriter.write(precord)
#            line = yield self._sreader.read(1024)
            line = ' '
            try:
                self._sock.send(precord)
                line = self._sock.recv(1024)
            except:
                logging.warn("lost connection to interposer")
                self._sock = None
                raise StopIteration
            if (not line): 
                logging.warn("lost connection to interposer")
                self._sock = None
                raise StopIteration
            self._recbuff = []



logging.basicConfig()
opts = collections.defaultdict(bool)
cmd_line_setters = {
    'U' : ('unix', None),
    'P' : ('packsize', int),
    'Z' : ('gzip', None),
    'N' : ('newfile', None),
    'r' : ('realtime', None)
}


argv = sys.argv[1:]
flist = []
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
            sys.stderr.write("Unrecognized option: %s\nUsage: %s -[%s]\n" % (arg, __file__, ''.join(cmd_line_setters.keys())))
            sys.exit(1)
    else:
        flist.append(arg)


if (len(flist) == 0):
    sys.stderr.write("Usage: %s -[%s] file1, file2.. \n" % (arg, __file__, ''.join(cmd_line_setters.keys())))
    sys.exit(1)


intersink = InterposerSink(**opts)
foo = intersink.connect()

if (opts['newfile']):
    nfspout = NewFileSpout(flist, **opts)
    nfspout.addsink(intersink)
    nfspout.run()
elif (opts['gzip']):
    gzspout = GZipSpout(flist)
    gzspout.addsink(intersink)
    gzspout.run()
else:
    s3c = boto.connect_s3()
    suff = ("19_00",)
#suff = ("19_00", "19_15", "19_30", "19_45", "20_00", "20_15")
    s3bucket = "affine-hadoop-input"
    s3keybase = "ads_servers/ads-434d7263/events/events.log.2014-03-20_"
    buck = s3c.get_bucket(s3bucket)
    for ssuf in suff:
        s3keyname = s3keybase + ssuf + ".gz"
        s3key = buck.get_key(s3keyname)
        if (not s3key):
            s3_log.error("cannot find S3 key %s in %s bucket" % (s3keyname, s3bucket))
            continue
        s3spout = S3FileSpout(s3key)
        s3spout.addsink(intersink)
        logging.warn("Starting S3 file spout ... connected to interposer")
        s3spout.run()




