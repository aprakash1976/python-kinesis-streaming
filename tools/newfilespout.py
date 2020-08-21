from streaming.bolt import BoltBase, SpoutBase
import sys
import logging
import msgpack
import streaming.msgmodel as msgmodel
import streaming.einterposer
import calendar
import datetime
import time


nf_log = logging.getLogger('newfilespout')

class NewFileSpout(SpoutBase):
    def __init__(self, flist, *args, **kwargs):
        super(NewFileSpout, self).__init__(*args, **kwargs)
        self._flist = flist
        self._realtime = kwargs.get('realtime', False)
        self._evtcount = 0
        self._t0 = time.time()

    def _get_next_event(self):
        checkintvl = 200  # millisecs
        clock0 = datetime.datetime.now()
        t00 = None
        emlast = None
        for fname in self._flist:
            with open(fname, "rb") as evt_stream:
                unpacker = msgpack.Unpacker(evt_stream)
                for evt in unpacker:
                    evt = msgmodel.expand(evt)
                    self._evtcount += 1
                    tnow = datetime.datetime.strptime(evt["timestamp_str"], "%Y-%m-%d %H:%M:%S")
                    if (t00 is None): t00 = tnow
                    elap = tnow - t00
                    if (self._realtime) and (elap.seconds != emlast):
                        clock_elap = datetime.datetime.now() - clock0
                        skew = (elap - clock_elap).total_seconds()
                        if (skew > 0.0):
                            time.sleep(skew)
                        emlast = elap.seconds
                    if ((self._evtcount % 10000) == 0):
                        nf_log.warn("evtcount: % 10d  eventelap: %ds   elap: %.1fs   %s" %
                                    (self._evtcount, elap.total_seconds(), (time.time() - self._t0), 
                                     evt.get('impression_id','_')))
                    yield evt

if __name__ == "__main__":
    import sys

    class DebugBolt(BoltBase):
        def __init__(self, *args, **kwargs):
            super(DebugBolt, self).__init__(*args, **kwargs)
            self._pcount = 0

        def process(self, data):
            if ('impression_id' not in data):
#                nf_log.warn("NOT ***** %s" % str(data))
                pass
            else:
                if (self._pcount < 100):
                    self._pcount += 1
                    nf_log.info("I: %s" % data.get('impression_id', '_'))
            return None

    logging.basicConfig()
    nf_log.setLevel(logging.INFO)
    f = NewFileSpout(sys.argv[1])
    q = DebugBolt(label="fake")
    f.addsink(q)
    nf_log.info("starting newfile spout")
    f.run()


        
        
