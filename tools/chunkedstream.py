import os
import json
import time
import asyncio
import concurrent.futures


#
#  Models an ordered stream of objects using a list of files
#    + Tail of the stream is writable (append-only).
#    + The stream is non-blocking lazy-loaded
#    + Use an iterator to traverse stream from .get_iterator()
#    + Iterator has string format that is always valid (i.e. can be stored and used in future)
#    + Iterator may raise StopIteration mid-stream if the data is NOT resident, 
#      Consumer needs to wait for asynch load to complete and re-try, Use .eos to test
#
#  e.g.
#        siter = chunkedstream.get_iterator()
#        while (not siter.eos):
#          for x in siter:
#             process(x)
#          if (not siter.eos): time.sleep(0.01)
#

def write_filechunk(filechunk):
    filechunk._flush()

def load_filechunk(filechunk):
    filechunk._load()


class WriteableChunk(object):
    def __init__(self, *args, **kwargs):
        self._data      = []
        self._next_fref = None
        self._ro        = False
        self._tupdate   = None
        
    @property
    def ro(self):
        return self._ro

    @property
    def nextfref(self):
        return self._next_page

    @nextfref.setter
    def nextfref(self, page):
        self._next_fref = page

    def append(self, value):
        if (self._ro):
            raise TypeError("'%s' object is immutable" % self.__class__.__name__)
        self._tupdate = (time.time())
        self._data.append(value)

    def get_data(self):
        return self._data

    def __getitem__(self, index):
        return self._data[index]

    def __len__(self):
        return len(self._data)



class FileChunk(WriteableChunk):
    def __init__(self, *args, **kwargs):
        super(FileChunk, self).__init__(*args, **kwargs)
        self._ro = True
        self._loop = kwargs['loop']
        self._pool = kwargs['pool']
        self._loadreq     = 0
        if ('path' in kwargs):
            self._path        = kwargs['path']
            (tupdate, seqs, count)  = os.path.basename(self._path).split(".")
            self._seqnum      = int(seqs, 16)
            self._count       = int(count)
            self._tupdate     = int(tupdate, 16)
            self._data        = None
        elif ('chunk' in kwargs):
            chunk = kwargs['chunk']
            self._seqnum      = kwargs['seqnum']
            self._data        = chunk.get_data()[:]
            self._count       = len(self._data)
            self._tupdate     = chunk._tupdate
            self._path        = os.path.join(kwargs['dir'], "%07x.%08x.%d" % (self._tupdate, self._seqnum, self._count))
            self._loop.run_in_executor(self._pool, write_filechunk, self)
            
    @property
    def resident(self):
        return (self._data is not None)

    def release(self):
        del self._data
        self._data = None

    def _load(self):
        if (self.resident): return
        tmp = []
        kpf = open(self._path, "r")
        while (True):
            rline = kpf.readline().strip("\n")
            if (len(rline) == 0): break
            element = json.loads(rline)
            tmp.append(element)
        kpf.close()
        self._ro = True
        self._data = tmp

    def _flush(self):
        kpf = open(self._path, "w")
        for element in self._data:
            kpf.write(json.dumps(element) + "\n")
        kpf.close()
        self._ro = True
        
    def __len__(self):
        return self._count

    def __getitem__(self, index):
        if (not self.resident):
            if (not self._loadreq):
                self._loop.run_in_executor(self._pool, load_filechunk, self)
            self._loadreq += 1
            return None
        return self._data[index]

    def __str__(self):
        return "<" + self.__class__.__name__ + " " + str(self._seqnum) + ">"
    
        


class ChunkedStream(object):
    _default_chunklimit = 512
    '''
    Creates a byte stream from an ordered list of regular files
    '''
    def __init__(self, flist, *args, **kwargs):
        super(ChunkedStream, self).__init__()
        self._fc = []
        self._loop = kwargs.get('loop', asyncio.get_event_loop())
        self._pool = kwargs.get('pool', concurrent.futures.ThreadPoolExecutor(max_workers=3))
        self._chunklimit = kwargs.get('chunklimit', self._default_chunklimit)
        self._dirpath = kwargs.get('dirpath', '/var/tmp')
        self._flist = flist
        self._trunc_last_seqnum = (len(flist)==0) and kwargs.get('last_seqnum', 0) or None
        self._scanandload()
        self._wrchunk = WriteableChunk()

    def offsets2seqnum(self, bidx, boff):
        if (bidx < len(self._fc)):
            return self._fc[bidx]._seqnum + boff
        elif (bidx == len(self._fc)):
            return self.last_file_seqnum + min(boff, len(self._wrchunk))
        else:
            return self.last_file_seqnum + len(self._wrchunk)
        
    def seqnum2offsets(self, seqnum):
        for bidx in xrange(len(self._fc)):
            fc = self._fc[bidx]
            if (seqnum >= fc._seqnum) and (seqnum < (fc._seqnum + fc._count)):
                boff = seqnum - fc._seqnum
                return (bidx, boff)
        bidx = len(self._fc)
        boff = min((seqnum - self.last_file_seqnum), len(self._wrchunk))
        return (bidx, boff)

    def _scanandload(self):
        oldfref = None
        for fname in self._flist:
            fref = FileChunk(path=fname, loop=self._loop, pool=self._pool)
            self._fc.append(fref)
            if (oldfref):
                oldfref.next = fref
            oldfref = fref

    def additem(self, item):
        if (len(self._wrchunk) == self._chunklimit):
            self.flush()
        location = self.last_seqnum
        self._wrchunk.append(item)
        return location

    def get_iterator(self, *args):
        seqnum = (len(args)>0) and int(args[0]) or self.first_seqnum
        return ChunkedStreamIterator(self, seqnum)

    def flush(self):
        if (len(self._wrchunk) > 0):
            fref = FileChunk(dir=self._dirpath, chunk=self._wrchunk, seqnum=self.last_file_seqnum,
                             loop=self._loop, pool=self._pool)
            self._fc.append(fref)
            self._wrchunk = WriteableChunk()
            
    def shutdown(self):
        self.flush()
        self._pool.shutdown()

    @property
    def first_seqnum(self):
        return (len(self._fc)>0) and self._fc[0]._seqnum or 0
            
    @property
    def last_file_seqnum(self):
        if (len(self._fc) == 0):
            return (self._trunc_last_seqnum is not None) and self._trunc_last_seqnum or 0
        return self._fc[-1]._seqnum + self._fc[-1]._count

    @property
    def last_seqnum(self):
        return self.last_file_seqnum + len(self._wrchunk)

    def __getitem__(self, index):
        if (index == -1) or (index == len(self._fc)):
            return self._wrchunk
        idx = (index < 0) and (index + 1) or index
        return self._fc[idx]
    
    def __len__(self):
        return len(self._fc) + 1


class ChunkedStreamIterator(object):
    def __init__(self, chunkstream, seqnum, *args, **kwargs):
        self._chunkstream = chunkstream
        mseqnum = (seqnum is None) and chunkstream.first_seqnum or seqnum
        (bidx, boff) = chunkstream.seqnum2offsets(int(mseqnum))
        self._bidx = bidx
        self._boff = boff

    @classmethod
    def fromoffsets(cls, chunkstream, bidx, boff):
        return cls(chunkstream, chunkstream.offsets2seqnum(bidx, boff))
    
    @classmethod
    def dup(cls, csiterator):
        return cls(csiterator._chunkstream, csiterator.seqnum)

    def __iter__(self):
        return self
    
    @property
    def seqnum(self):
        return self._chunkstream.offsets2seqnum(self._bidx, self._boff)
    
    @property
    def eos(self):
        return (self._bidx == len(self._chunkstream))
        
    def next(self):
        while (True):
            if (self._bidx == len(self._chunkstream)):
                raise StopIteration
            p = self._chunkstream[self._bidx]
            if (self._boff < len(p)):
                val = p[self._boff]
                if (val is None):
                    raise StopIteration
                self._boff += 1
                return val
            self._bidx += 1
            self._boff = 0

    def __add__(self, delta):
        boff = self._boff
        for bidx in xrange(self._bidx, len(self._chunkstream)):
            p = self._chunkstream[bidx]
            if ((len(p) - boff) > delta):
                return ChunkedStreamIterator.fromoffsets(self._chunkstream, bidx, (boff+delta))
            delta -= len(p) - boff
            boff = 0
        return ChunkedStreamIterator.fromoffsets(self._chunkstream, len(self._chunkstream)-1, len(self._chunkstream[-1]))

    def __str__(self):
        return "%d" % self.seqnum
    
            


            

if __name__ == "__main__":
    import sys
    dpath = "/home/adrian/kin_data/AdsQueryRaw.d"
    afiles = [ os.path.join(dpath, fname) for fname in os.listdir(dpath) ]
    bfs = ChunkedStream(sorted(afiles))
    qq = WriteableChunk()
    cnt = 0
    stream_iter = bfs.get_iterator(190)
    last0 = bfs.last_seqnum
    print 'Last seqnum after disk recovery', bfs.last_seqnum
    print 'Iterator seqnum', stream_iter.seqnum
    while (not stream_iter.eos):
        for zz in stream_iter:
            (ts, key, data) = zz
            last_data = data
            cnt += 1
            if (cnt <= 100):
                tnew = int(time.time())
                bfs.additem((tnew, key, data))
                qq.append((tnew, key, data))
                last_data_stored = data
        if (not stream_iter.eos):
            time.sleep(0.01)

    assert((last0+100) == bfs.last_seqnum)
    print 'EOS ', stream_iter.eos
    print 'Last seqnum after adding 100 items', bfs.last_seqnum
    print 'Stream Iterator EOS string', str(stream_iter), len(bfs)
    print 'Iterator seqnum', stream_iter.seqnum
    print '\n\n', last_data_stored
    print '\n', last_data

    stream_iter = bfs.get_iterator(198)
    y = stream_iter + 10
    assert((y.seqnum == (stream_iter.seqnum+10)))
    for y in stream_iter:
        pass
    print stream_iter.seqnum, stream_iter.eos
    assert(stream_iter.eos)

    cnt = 0
    si = bfs.get_iterator()
    print 'Default iterator (starts at beginning)', si
    assert((si.seqnum == 0))
    while (not si.eos):
        for x in si:
            cnt += 1
        if (not si.eos):
            time.sleep(0.05)
    print cnt, bfs.last_seqnum
    print 'Iterator @ EOS', si
    assert((cnt == si.seqnum))
    assert((bfs.last_seqnum == cnt))

    bfs.shutdown()
    sys.exit(0)
        
        
