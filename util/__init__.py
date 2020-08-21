from UserDict import UserDict
from threading import Timer, Lock, Thread
import imp
import os
import time
from datetime import datetime
from collections import Counter
import logging


def snaphour(now=None):
    if (not now):
        now = datetime.utcnow()
    v = now.timetuple()[0:4] + (0,)*2
    tnow = datetime(*v)
    return tnow


def shardid2index(shardid):
    if (shardid.find("-")>=0):
        return int(shardid.split("-")[1])
    return 0


class TimeWinStore(UserDict):
    """ Store objects in a limited time window,
        Delete objects as they age out
    """
    def __init__(self, window_size, *args, **kwargs):
        UserDict.__init__(self)
        self._window = {}
        self._wsize = window_size
        self._tzero = None
        self._tinit = None
        self._purge_handler = kwargs.get('handler', None)
        self._wraparound = False
        self._ooo = 0
        
    def _check_and_purge(self, timestamp):
        while ((timestamp - self._tzero) > self._wsize):
            if (self._tzero in self._window):
                evictions = []
                for key in self._window[self._tzero]:
                    evictions.append((key, self.data[key]))
                    del self.data[key]
                del self._window[self._tzero]
                if (self._purge_handler):
                    self._purge_handler.purge(evictions)
            self._tzero += 1

    def timed_add(self, timestamp, key, thing):
        if (self._tinit is None):
            self._tzero = timestamp - self._wsize
            self._tinit = timestamp

        if (timestamp < self._tzero):
            self._ooo += 1
            return 1

        if (timestamp not in self._window):
            self._check_and_purge(timestamp)
            self._window[timestamp] = []
        self._window[timestamp].append(key)
        self.data[key] = thing
        return 0

    def __len__(self):
        return reduce((lambda x,y: x+len(y)), self._window.values(), 0)



def intervalwrapper(fid):
    fid.dispatch()

class IntervalDispatcher(Thread):
    '''
    Schedule an object's method to be periodically invoked at a fixed time interval.
    Fires when the unix timestamp is a multiple of the time interval
    '''
    def __init__(self, interval, targetobj, methodname, **kwargs):
        super(IntervalDispatcher, self).__init__()
        self._interval  = interval
        self._targetobj = targetobj
        self._targetmethod = getattr(targetobj, methodname)
        self._enabled = True
        self._started = False
        self.daemon = kwargs.get('daemon', False)
        self._logger = kwargs.get('logger', logging.getLogger("__main__"))
    
    def _tresidue(self):
        t1 = time.time()
        it1 = int(t1)
        mt = (it1/self._interval + 1) * self._interval + 0.0
        return mt - t1

    def cancel(self):
        self._enabled = False

    def dispatch(self):
        if (not self._started):
            self._started = True
            self.start()

    def run(self):
        while (self._enabled):
            try:
                self._targetmethod()
            except:
                self._logger.exception("caught exception on {0}.{1}".format(self._targetobj.__class__.__name__, self._targetmethod.__name__))
            if (self._enabled):
                tresidue = self._tresidue()
                time.sleep(tresidue)


def find_dotted_module(name, path=None):
    '''
    use python internal module search to recursively find
    the path (returns None if the module cannot be found by 
    regular import heuristics).
    '''
    for frag in name.split('.'):
        searchpath = (path is not None) and [path] or None
        try:
            (mfile, path, desc) = imp.find_module(frag, searchpath)
        except ImportError:
            return None
        else:
            if (mfile):
                mfile.close()
    return path
    

def import_fullpath(modpath):
    '''
    import a module (including hierachical). Returns None on error
    '''
    leafmodule, e = os.path.splitext(os.path.basename(modpath))
    try:
        (mfile, mpath, desc) = imp.find_module(leafmodule, [os.path.dirname(modpath)])
    except ImportError:
        pass
    else:
        print mfile, mpath
        try:
            modobj = imp.load_module(leafmodule, mfile, mpath, desc)
        except ImportError:
            pass
        else:
            if (mfile):
                mfile.close()
            return modobj
    return None




class TCounter(Counter):
    def __init__(self, *args, **kwargs):
        super(TCounter, self).__init__(*args, **kwargs)
        self._tclock = Lock()
    
    def incr(self, key):
        self._tclock.acquire()
        self[key] += 1
        self._tclock.release()

    def set(self, key, val):
        self._tclock.acquire()
        self[key] = val
        self._tclock.release()
        
    def reset_all(self):
        self._tclock.acquire()
        for k in self.keys():
            self[k] = 0
        self._tclock.release()

    def pprint(self):
        return ','.join(k+'='+str(v) for (k,v) in self.iteritems())






class SegRanges(object):
    #
    #  Given a sequence of integers, this object groups consecutive
    #  integers into ordered ranges. I.e. [(x0,x1), (x2,x3)] implies that
    #  all integers in range(x0,x1+1) and range(x2,x3+1) were added.
    #  Also note, ranges are disjoint, x2 > x1+1
    #
    #  set
    #
    def __init__(self, *args, **kwargs):
        self._grain = len(args)>0 and int(args[0]) or 1
        self._segs = kwargs.get("segments", [])

    def add(self, x):
        for (idx,bounds) in enumerate(self._segs):
            if (x < (bounds[0]-self._grain)):
                self._segs.insert(idx, [x,x])
                return
            elif (x <= (bounds[1]+self._grain)):
                bounds[0] = min(x, bounds[0])
                bounds[1] = max(x, bounds[1])
                if ((idx+1) < len(self._segs)) and ((bounds[1]+self._grain) == self._segs[idx+1][0]):
                    bounds[1] = self._segs[idx+1][1]
                    del self._segs[idx+1]
                return
        self._segs.append([x, x])

    def __iter__(self):
        for seg in self._segs:
            yield seg

    def __len__(self):
        return len(self._segs)









def args_parser(args, cfg):
    """

    args_cfg = {
        "direct" : {
            "switch" : True,
        },
        "interactive" : {
            "switch" : True,
            "shortcut" : "I"
        },
        "foo" : {
            "tovalue" : int
        },
        "crap" : {
            "shortcut" : "C"
        }
    }

    """
    options = {}
    long_args = set(cfg.keys())
    short_map = dict(filter((lambda v: v[0]), ((cfg[k].get("shortcut"),k) for k in cfg.keys())))
    short_args = set(short_map.keys())
    for kopt in cfg.keys():
        if "switch" in cfg[kopt]:
            options[kopt] = False
    idx = 0
    unknown_args = []
    while (idx < len(args)):
        if (args[idx].startswith("--")):
            v = args[idx][2:].split("=")
            kopt = v[0]
            kval = len(v)>1 and v[1] or None
            if (kopt in long_args):
                if ("switch" in cfg[kopt]):
                    options[kopt] = True
                else:
                    try:
                        options[kopt] = cfg[kopt].get("tovalue", (lambda x: x))(kval)
                    except:
                        return (None, None)
            else:
                unknown_args.append(args[idx])

        elif (args[idx].startswith("-")):
            shortcut = args[idx][1:]
            if (shortcut in short_args):
                kopt = short_map[shortcut]
                if ("switch" in cfg[kopt]):
                    options[kopt] = True
                else:
                    if (len(args[idx+1:])==0):
                        return (None, None)
                    idx += 1
                    kval = args[idx]
                    try:
                        options[kopt] = cfg[kopt].get("tovalue", (lambda x: x))(kval)
                    except:
                        return (None, None)
            else:
                unknown_args.append(args[idx])
        else:
            unknown_args.append(args[idx])
        idx += 1
    return options,unknown_args
        
