import collections
import logging
import exceptions
#
#    Base classes to build in-process consumer/producer pipelines
#    Using Storm terminology: 
#         bolt  - a stage in the pipeline that recieves data and possibly emits data
#         spout - a type of bolt that only emits data
#
#    A bolt subscribes to another bolt's output by adding itself with the producer's
#    the addsink() method. Multiple bolts can subscribe to the same producer thereby
#    replicating the data stream. 
#
#    The entire pipeline runs single-threaded. In particular, data & control are 
#    propagated depth-first through the topology. Bolts should not assume any ordering
#    w.r.t. any sibling bolts recieving data. Ideally, bolts should be entirely decoupled
#    from sibling and subscriber bolts.
#
#    The process() method is invoked on reciept of each data element. Subclasses 
#    implement process() method to do specific work. Any return value from process() 
#    except None is propagated to any subscribers.
#
#    Ideally, each bolt should be stateless and designed to run indefinitely. However, a bolt 
#    may unilaterally gracefully stop processing by raising StopIteration. This will disengage 
#    the  bolt from the upstream producer and call the shutdown() method. Bolts should implement
#    the shutdown() method to do any specific cleanup actions. Additionally, subscriber
#    bolts shutdown() methods are also invoked. Upstream and sibling bolts are unaffected.
#
#

class BoltRuntimeError(StandardError):
    pass


class BoltTerminate(BoltRuntimeError):
    """
    Used by downstream bolts to terminate entire pipeline
    """
    pass


class BoltDetach(BoltRuntimeError):
    """
    Used by downstream bolts to detach -- request stop sending events
    """
    pass




def coroutine(func):
    def start(*args, **kwargs):
        g = func(*args, **kwargs)
        g.next()
        return g
    return start


class BoltBase(object):
    def __init__(self, *args, **kwargs):
        super(BoltBase, self).__init__()
        self._label  = kwargs.get("label", id(self))
        self._sinks  = []
        if ('sinks' in kwargs):
            self.addsink(kwargs['sinks'])
        self._delegate = self._receiver()
        self._active   = True
        self._log = kwargs.get('logger', logging.getLogger("__main__"))

    def _process(self, data):
        try:
            rval = self.process(data)
        except StopIteration:
            self._log.warn("{0} caught downstream StopIteration".format(self.__class__.__name__))
            self._wrap_shutdown()
            raise StopIteration
        except BoltTerminate:
            self._log.warn("{0} caught downstream BoltTerminate".format(self.__class__.__name__))
            self._wrap_shutdown()
            raise 
        except Exception as e:
            self._log.exception("exception in processing")
            raise
        if (rval is not None):
            self._dispatch(rval)
        return

    @coroutine
    def _receiver(self):
        while (True):
            data = (yield)
            if (data) is None:
                self._wrap_shutdown()
            else:
                self._process(data)

    def _wrap_shutdown(self):
        self._dispatch(None)
        self.shutdown()
        self._active = False

    def _dispatch(self, data):
        for sink in self._sinks:
            if (sink['active']):
                try:
                    sink['bolt'].send(data)
                except StopIteration:
                    sink['active'] = False

    def send(self, data):
        self._delegate.send(data)

    def addsink(self, sink):
        sinks = isinstance(sink, collections.Iterable) and sink or [sink]
        for s in sinks:
            self._sinks.append({'bolt': s, 'active':True})

    def process(self, data):
        return data

    def shutdown(self):
        return

    def cancel(self):
        self._wrap_shutdown()

    def info(self):
        return {'label':self._label, 'sink_count' : len(self._sinks)}



class SpoutBase(BoltBase):
    def __init__(self, *args, **kwargs):
        super(SpoutBase, self).__init__(*args, **kwargs)

    def _reciever(self):
        return (lambda *args: None)

    def _get_next_event(self):
        while (True):
            yield self.get_next_event()

    def run(self):
        for data in self._get_next_event():
            if (data is not None):
                try:
                    self._dispatch(data)
                except BoltTerminate:
                    self._log.warn("{0} Caught BoltTerminate - stopping main spout loop".format(self.__class__.__name__))
                    break
        self._dispatch(None)



class DemuxBase(BoltBase):
    def __init__(self, *args, **kwargs):
        super(DemuxBase, self).__init__(*args, **kwargs)

    def _shard_key(self, data):
        return hash(data)

    def _shard_id(self, data):
        if (self._sinks):
            return self._shard_key(data) % len(self._sinks)
        return 0

    def _dispatch(self, data):
        if (data is not None):
            shard_id = self._shard_id(data)
            sink = self._sinks[shard_id]
            if (sink['active']):
                try:
                    sink['bolt'].send(data)
                except StopIteration:
                    sink['active'] = False
        else:
            for sink in self._sinks:
                if (sink['active']):
                    try:
                        sink['bolt'].send(data)
                    except StopIteration:
                        sink['active'] = False

    
    
    


if __name__ == "__main__":
    import time

    class DebugBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(DebugBolt, self).__init__(*args, **kwargs)
            self._counter = 0

        def process(self, data):
            self._counter += 1
            print self._label, self._counter
            return data

        def shutdown(self):
            pass

        
    class MyBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(MyBolt, self).__init__(*args, **kwargs)
            self._counter = 0

        def process(self, data):
            self._counter += 1
            if (self._counter > 10):
                self._counter = 0
                raise StopIteration
            return data

        def shutdown(self):
            self._counter = 0


    class MySpout(SpoutBase):
        names = [ "bob", "evelyn", "jack", "kyle", "sarah", "ursula", "chris", "robert"]
        def __init__(self, *args, **kwargs):
            super(MySpout, self).__init__(*args, **kwargs)
            self._counter = 0

        def get_next_event(self):
            self._counter += 1
            return self.names[self._counter%len(self.names)]

    p = MyBolt(label="L1.0")
    q = DebugBolt(label="L0.0")
    p.addsink(q)
    s = DebugBolt(label="L2.0")
    s.addsink(p)
    k = DebugBolt(label="L2.1")
    r = DebugBolt(label="L3.0")
    r.addsink(s)
    r.addsink(k)
    e = MySpout()
    e.addsink(r)
    e.run()

#    r.send("crap")
#    r.send("balls")





