from datetime import datetime, timedelta
import calendar
import time
import logging
import sys
from streaming.infobright_rollup import InfobrightRollup
from streaming.util.s3evtmsgs import S3EvtmsgSpout3
from streaming.bolt import BoltBase
from streaming.adservcoalesce import AdServCoalesce

class TesterBolt(BoltBase):
    def __init__(self, *args, **kwargs):
        super(TesterBolt, self).__init__(*args, **kwargs)
        self._view_count = 0
        self._last_time_str = None
        self._last_mark = 0
        self._t0 = time.time()
        self._first_ts = None
        
    def process(self, rec):
        elist = rec['event_list']
        self._view_count += any(((etype == 'view') for (etype,timestamp) in elist))
        if (rec['timestamp'] != self._last_time_str):
            self._last_time_str = rec['timestamp']
            dtime = datetime.strptime(rec["timestamp"], "%Y-%m-%d %H:%M:%S")
            if (self._first_ts is None):
                self._first_ts = dtime
            self._last_ts = calendar.timegm(dtime.timetuple())
            if ((self._last_ts > self._last_mark) and ((self._last_ts % 600) == 0)):
                self._last_mark = self._last_ts
                #                    print dtime, "{0:.0f} secs".format((time.time() - self._t0))
            print '{0}  event_clock elapsed: {1:.0f}    wall_clock elapsed: {2}'.format(dtime,
                                                                                        (dtime-self._first_ts).total_seconds(),
                                                                                        int((time.time() - self._t0)))

if __name__ == "__main__":
    opts = {}
    argv = sys.argv[1:]
    cmd_line_setters = {
        'S' : ('start', (lambda x: x)),
        'E' : ('end', (lambda x: x)),
        'f' : ('force', None),
        'L' : ('librato_label', (lambda x: x))
        }

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
                sys.stderr.write("Unrecognized option: " + arg + "\n")
                sys.exit(1)
    
    if ('start' not in opts) or ('end' not in opts):
        sys.stderr.write('Usage: backfill_infobright -S "YYYY-mm-dd HH:MM" -E "YYYY-mm-dd HH:MM"\n')
        sys.exit(1)
        
    tstart = datetime.strptime(opts["start"], "%Y-%m-%d %H:%M")
    tend   = datetime.strptime(opts["end"], "%Y-%m-%d %H:%M")
    s3_start = tstart
    s3_end = tend
    if (not opts.get('force')):
        s3_start = datetime(tstart.year, tstart.month, tstart.day, tstart.hour) + timedelta(minutes=5)
        s3_end   = datetime(tend.year, tend.month, tend.day, tend.hour) + timedelta(minutes=5)

    _log = logging.getLogger("__main__")
    _log.info("Run from {0} to {1}".format(tstart, tend))
    backfill_loader = InfobrightRollup(logger=_log, shard_id='11', back_fill=True, 
                           tstart=tstart, tend=tend, librato_label=opts.get('librato_label',''))
    adscoalesc = AdServCoalesce(timewindow=(4*60), print_granul=1000000)
    adscoalesc.addsink(backfill_loader)
    s3spout = S3EvtmsgSpout3(s3_start, s3_end, ishard=0, nshard=1, logger=_log)
    s3spout.addsink(adscoalesc)
    t0 = time.time()
    s3spout.run()
    _log.info("Elapsed {0:.0f} secs".format((time.time() - t0)))
    sys.exit(0)
    #backfill_loader.dumper()

