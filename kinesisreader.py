import boto
import time
import logging
import msgpack
import asyncio
import concurrent.futures
import Queue
import base64
from streaming import msgmodel

def reader_start(shard_reader):
    shard_reader.start()
 
class KinesisShardReader(object):
    _MAX_QUEUE_SIZE = 20
    def __init__(self, stream, shardid, fifo, *args, **kwargs):
        self._log = kwargs.get('logger', logging.getLogger(self.__class__.__name__))
        self._fifoq = fifo 
        self._kin = boto.connect_kinesis()
        stream_info = self._kin.describe_stream(stream)
        self._stream = stream_info['StreamDescription']
        self._shardinfo = filter((lambda x: x['ShardId'] == shardid), self._stream['Shards'])[0]
        self._log.debug('shardinfo {0}'.format(str(self._shardinfo)))
        self._maxqsize = kwargs.get('maxqsize', self._MAX_QUEUE_SIZE)
        self._max_retries = 3
        self._rec_chunk = 1000
        self._poll_interval = 2
        self._stop = False
        self._alive = True
        self._start_at = kwargs.get("start_at", "LATEST")
    
    def __str__(self):
        return self._shardinfo['ShardId']

    def _get_records(self):
        for ktry in xrange(2):
            res = None
            for retry in xrange(self._max_retries):
                try:
                    res = self._kin.get_records(self._shard_iter, limit=self._rec_chunk, b64_decode=False)
                except Exception as e:
                    logging.warn('trapped error on Kinesis, attempt {0}/{1}  {2}'.format(retry, self._max_retries, e))
                else:
                    break
            if (res): break
            if (ktry != 0): return -1
            self._log.warn('Exhausted retries. Resetting shard interator!')
            rsi = self._kin.get_shard_iterator(self._stream['StreamName'], self._shardinfo['ShardId'], "LATEST")
            self._shard_iter  = rsi['ShardIterator']

        self._log.debug('ShardReader: record count: {0}   qsize: {1}'.format(len(res['Records']), self._fifoq.qsize()))
        self._shard_iter = res['NextShardIterator']
        recv = []
        for rec in res['Records']:
            bindata = base64.b64decode(rec['Data'])
            recbundle = msgpack.unpackb(bindata)
            for msg in recbundle:
                recv.append(msgmodel.expand(msg))
        if (len(recv)):
            if (self._fifoq.qsize() < self._maxqsize):
                self._fifoq.put(recv)
            else:
                self._log.warn("Exceeded queue size limit.  dropping records")

    def stop(self):
        self._stop = True

    def start(self):
        self._log.info('Thread started {0}'.format(str(self)))
        rsi = self._kin.get_shard_iterator(self._stream['StreamName'],
                                           self._shardinfo['ShardId'],
                                           self._start_at)
        self._shard_iter  = rsi['ShardIterator']
        self._log.info("{1} ShardIterator: {0}".format(str(self._shard_iter), self._stream['StreamName']))
        while (not self._stop):
            time.sleep(self._poll_interval)
            if (self._stop): break
            self._get_records()
        self._log.warn('ShardReader {0} exiting'.format(str(self)))
        self._alive = False
        return

    @property
    def alive(self):
        return self._alive


class KinesisReader(object):
    def __init__(self, stream, shardindex, *args, **kwargs):
        self._log = kwargs.get('logger', logging.getLogger(self.__class__.__name__))
        self._log.setLevel(kwargs.get('loglevel', logging.INFO))
        self._fifoq = Queue.Queue()
        self._kin = boto.connect_kinesis()
        stream_info = self._kin.describe_stream(stream)
        self._stream = stream_info['StreamDescription']
        self._set_shards(shardindex)
        self._thrpool = kwargs.get('thrpool', concurrent.futures.ThreadPoolExecutor(max_workers=(len(self._shard_readers)+1)))
        self._callbacks = []
        self._futures = []
        self._evtcount = 0
        self._state = 'stopped'
        self._enabled = True

    def __str__(self):
        return "<{0} stream:{1}>".format(self.__class__.__name__, self._stream['StreamName'])

    def _start_readers(self):
        for sr in self._shard_readers:
            ft = self._thrpool.submit(reader_start, sr)
            self._futures.append(ft)
        
    def shutdown(self):
        self._log.warn('KinesisReader shutting down -- Total events recieved {0}'.format(self._evtcount))
        for shard_reader in self._shard_readers:
            shard_reader.stop()
        self._enabled = False

    def getnb(self):
        try:
            evt = self._fifoq.get(False)
        except Queue.Empty:
            raise
        self._evtcount += 1
        return evt

    def _set_shards(self, shardindex):
        self._stream['Shards'] = sorted(self._stream['Shards'], key=(lambda x: x['ShardId']))
        scount = len(self._stream['Shards'])
        if (shardindex is None):
            shidx = range(0, len(self._stream['Shards']))
        elif (isinstance(shardindex, tuple) or isinstance(shardindex, list)):
            shidx = shardindex
        else:
            shidx = (int(shardindex),)
        self._shidx = tuple(filter((lambda x: x>=0 and x<scount), set(shidx)))
        if (not self._shidx):
            raise IndexError("no valid shard indices supplied: {0}".format(str(shardindex)))

        maxqsize = len(self._shidx)*10
        self._shard_readers = []
        for shi in self._shidx:
            sss = self._stream['Shards'][shi]
            self._log.info('Reading from shard [{0}] : {1}'.format(sss['ShardId'], self._stream['StreamName']))
            sr = KinesisShardReader(self._stream['StreamName'], sss['ShardId'], self._fifoq, logger=self._log,
                                    maxqsize=maxqsize)
            self._shard_readers.append(sr)

    def start(self):
        self._log.info('Kinesis Reader starting shards')
        if (self._state == 'stopped'):
            self._start_readers()

    def alive(self):
        return all((not shard_reader.alive for shard_reader in self._shard_readers))

