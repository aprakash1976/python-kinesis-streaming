import asyncio
import sys
import json
import logging
import time
import socket
import os.path
from chunkedstream import ChunkedStream, ChunkedStreamIterator

ene_log = logging.getLogger('energeia')
default_port = 9461
default_host = '127.0.0.1'
default_fetch_count = 20
default_data_path = "/var/tmp"
MHEADER_LEN = 10


def flush_kpage(ro_page, fpath, stream):
    ff = open(fpath, "w")
    ro_page.flush(ff)
    ff.close()


class EnergeiaShardIterator(ChunkedStreamIterator):
    def __init__(self, stream, shard, seqnum, *args, **kwargs):
        super(EnergeiaShardIterator, self).__init__(stream, seqnum, *args, **kwargs)
        self._shard = shard
        self._name = stream._name
        self._width = stream._width

    def get_records(self, count):
        resp = []
        for record in self:
            (ts, key, rec) = record
            slot = hash(key) % self._width
            if (slot == self._shard):
                resp.append(rec)
                if (len(resp) == count):
                    break
        return resp
    
    def __str__(self):
        childcursor = super(EnergeiaShardIterator, self).__str__()
        return '.'.join((self._name, str(self._width), "%04d" % self._shard, childcursor))

        
class EnergeiaStream(ChunkedStream):
    def __init__(self, name, width, *args, **kwargs):
        super(EnergeiaStream, self).__init__(*args, **kwargs)
        self._name  = name
        self._width = width

    def _str2iter(self, iterstr):
        (name, width, shardid, seqnum) = iterstr.split(".")
        return EnergeiaShardIterator(self, int(shardid), int(seqnum))

    def get_shard_iterator(self, shardid, dummy, seqnum):
        return dict(ShardIterator=str(EnergeiaShardIterator(self, int(shardid), seqnum)))

    def get(self, shard_iterator, count):
        siter = self._str2iter(shard_iterator)
        records = siter.get_records(count)
        records = [ dict(Data=r) for r in records ]
        return dict(NextShardIterator=str(siter), Records=records)

    def put(self, record, key, explicit_key):
        ts = int(time.time())
        hk = (explicit_key is not None) and int(explicit_key) or hash(key)
        slot = hk % self._width
        seqnum = self.additem((ts, key, record))
        return dict(SequenceNumber=seqnum, ShardId="%04d" % slot)

    def info(self):
        shards = [ dict(ShardId="%04d" % ish, SequenceNumberRange=dict(StartingSequenceNumber=self.first_seqnum)) for ish in xrange(self._width) ]
        sinfo = dict(StreamName=self._name, Shards=shards)
        return dict(StreamDescription=sinfo)


class EnergeiaService(object):
    def __init__(self, *args, **kwargs):
        super(EnergeiaService, self).__init__()
        self._data_path = kwargs.get('data_path', default_data_path)
        self._streams = {}

    def prefetch_stream(self, name):
        strm = self._streams[name]
        resp = strm.get_shard_iterator(0, "sdsdsd", "")
        cnt = 0
        while (not siter.eos):
            v = siter.get_records(100)
            cnt += len(v)
            ene_log.debug("records length %d       sharditer: %s" % (len(v), str(siter)))
            if (len(v) == 0) and (not siter.eos):
                time.sleep(0.1)
        ene_log.debug("done with prefetch")

    def create_stream(self, name, width):
        sdir = os.path.join(self._data_path, name + ".d")
        if (not os.path.exists(sdir)):
            os.makedirs(sdir)
        files0 = sorted(os.listdir(sdir))
        last_file_seqnum = 0
        if (len(files0)):
            (ts, snum, count) = files0[-1].split(".")
            last_file_seqnum = int(snum, 16) + int(count)
            ene_log.info("last file seqnum: %d" % last_file_seqnum)
        t24 = int(time.time()) - (3600*24)
        files = filter((lambda file: int(file.split(".")[0], 16) > t24), files0)
        files = map((lambda x: os.path.join(sdir, x)), files)
        strm = EnergeiaStream(name, width, files, dirpath=sdir, chunklimit=256, last_seqnum=last_file_seqnum)
        self._streams[name] = strm
        ene_log.info("stream created %s %d    restored from %d files" % (name, width, len(strm)-1))

    def testng(self, name):
        strm = self._streams[name]
        si = strm.get_shard_iterator("0001", "sdsdsd", None)
        si = si['ShardIterator']
        ene_log.info("Beginning of Stream  Iterator: %s" % str(si))
        resp = strm.get(si, 10)
        ene_log.info("Stream Last Seqnum : %s" % strm.last_seqnum)
        ene_log.info("RESP : %s" % json.dumps(resp))
        time.sleep(1.0)
        resp = strm.get(si, 10)
        ene_log.info("RESP : %s" % json.dumps(resp))
        ene_log.info("Stream Iterator incremented: %s" % resp['NextShardIterator'])
        r0 = resp['Records'][-1]
        ene_log.info("Before put -- last seqnum: %d" % strm.last_seqnum)
        resp = strm.put(r0, r0['impression_id'])
        ene_log.info("PUT RESP : %s" % json.dumps(resp))
        resp = strm.get_shard_iterator(resp['ShardId'], "sdsdsd", resp['SequenceNumber'])
        ene_log.info("AFTER PUT -  EOS ITERATOR : %s" % json.dumps(resp))
        ene_log.info("After put -- last seqnum: %d" % strm.last_seqnum)
        ss = strm.get(resp['ShardIterator'], 1)
        s0 = ss["Records"][0]
        ene_log.info(" Put and retrieved same record: %s %s" % (r0['impression_id'], s0['impression_id']))

    def cmd_dispatch(self, cmd, dargs):
        err_resp = dict(status='error')
        if (cmd in ('GET',)):
            (streamname, dummy) = dargs['shard_iterator'].split('.',1)
            stream = self._streams[streamname]
        if (cmd in ('PUT', 'GETIT', 'INFO')):
            if ('streamname' not in dargs): return err_resp
            if (dargs['streamname'] not in self._streams): return err_resp
            stream = self._streams[dargs['streamname']]

        ene_log.debug("  CMD : " + cmd + "  <>  " + str(dargs))
        if (cmd == "GET"):
            resp = stream.get(dargs['shard_iterator'], dargs.get('limit', default_fetch_count))
        elif (cmd == "PUT"):
            ene_log.warn('Record length: {0}   {1}'.format(len(dargs['record']), ':'.join(dargs.keys())))
            resp = stream.put(dargs['record'], dargs['key'], dargs.get('explicit_hash_key', None))
        elif (cmd == "INFO"):
            resp = stream.info()
        elif (cmd == "GETIT"):
            resp = stream.get_shard_iterator(dargs['shard_id'], 
                                             dargs['shard_iterator_type'],
                                             dargs['starting_seqnum'])
        ene_log.debug("CMD: " + cmd + "  RESPONSE: " + str(resp))
        resp['status'] = 'OK'
        return resp

    def shutdown(self):
        ene_log.warn('Shutdown: flushing %d streams' % len(self._streams))
        for strm in self._streams.values():
            strm.shutdown()



class EnergeiaClient(object):
    def __init__(self, host=default_host, port=default_port, *args, **kwargs):
        self._host = host
        self._port = port
        self._bufsize = 1024
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self._host, self._port))

    def _send_req(self, cmd, **kwargs):
        msg = json.dumps(dict(cmd=cmd, args=kwargs))
        self._sock.send(msg+'\n')
        hdr = ''
        while (len(hdr) < MHEADER_LEN):
            chunk = self._sock.recv(MHEADER_LEN - len(hdr))
            hdr = hdr + chunk
        magic = hdr[:4]         # TODO: check magic string
        mlen = int(hdr[4:], 16)
        data = ''
        while (len(data) < mlen):
            chunk = self._sock.recv(mlen - len(data))
            data = data + chunk
        obj = json.loads(data.strip('\n'))
        status = obj['status']
        del obj['status']
        return obj

    def put_record(self, streamname, data, key, **kwargs):
        return self._send_req("PUT", streamname=streamname, key=key, record=data, **kwargs)

    def get_records(self, shard_iterator, **kwargs):
        fargs=dict(shard_iterator=shard_iterator)
        if ('limit' in kwargs): fargs['limit'] = kwargs['limit']
        return self._send_req("GET", **fargs)

    def get_shard_iterator(self, streamname, shard_id, shard_iterator_type, starting_sequence_number=None):
        starting_seqnum = starting_sequence_number is None and 0 or starting_sequence_number
        return self._send_req("GETIT", streamname=streamname, shard_id=shard_id, 
                              shard_iterator_type=shard_iterator_type, 
                              starting_seqnum=starting_seqnum)

    def describe_stream(self, streamname):
        return self._send_req("INFO", streamname=streamname)

    def close(self):
        self._sock.close()


                
if __name__ == "__main__":
    import logging

    def runner(streamlist, **kwargs):
        kin_svc = EnergeiaService(**kwargs)
        asyncio.tasks._DEBUG = True
        ene_log.setLevel(logging.INFO)
        if (kwargs.get('debug', False)):
            ene_log.setLevel(logging.DEBUG)

        def svr_dispatch(sreader, swriter):
            while (True):
                data = yield sreader.readline()
                if (len(data) == 0): break
                data = json.loads(data.strip('\n'))
                resp = kin_svc.cmd_dispatch(data['cmd'], data['args'])
                msg = json.dumps(resp) + '\n'
                hdr = "cafe%06x" % len(msg)
                msg = hdr + msg
                yield swriter.write(msg)

        for (stream_name, stream_width) in streamlist:
            kin_svc.create_stream(stream_name, stream_width)
            
        loop = asyncio.get_event_loop()
        svr = asyncio.start_server(svr_dispatch, 
                                   host=kwargs.get('host', default_host), 
                                   port=kwargs.get('port', default_port))
        loop.run_until_complete(svr)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            kin_svc.shutdown()
            raise
            


    logging.basicConfig()
    cmd_line_setters = {
        'D' : ('data_path', (lambda x: x)),
        'P' : ('port', int),
        'V' : ('debug', (lambda x: True))
        }
    streamlist = []
    opts = {}
    argv = sys.argv[1:]
    while (len(argv)):
        arg = argv.pop(0)
        if arg.startswith('-'):
            if (arg[1] in cmd_line_setters.keys()):
                (label, transform) = cmd_line_setters[arg[1]]
                val = argv.pop(0)
                opts[label] = transform(val)
            else:
                sys.stderr.write("Unrecognized option: " + arg + "\n")
                sys.exit(1)
        else:
            stream_name = arg.split(':')[0]
            stream_width = (':' in arg) and int(arg.split(':')[1]) or 1
            streamlist.append((stream_name, stream_width))
    runner(streamlist, **opts)
