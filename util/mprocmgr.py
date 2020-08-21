import sys
import os
import time
import subprocess32
import signal
import logging
import re
import daemon
import inspect
import threading
from daemon.pidfile import TimeoutPIDLockFile
from streaming.util import args_parser

mgrobj = None


def daemon_TERM_handler(signum, frame):
    xlog = logging.getLogger("__main__")
    if (mgrobj) and (getattr(mgrobj,"_daemon_TERM_handler")):
        xlog.warn("Propagating TERM signal to mgr obj {0}".format(mgrobj.__class__.__name__))
        getattr(mgrobj, "_daemon_TERM_handler")(signum)
    xlog.warn('About to exit threadcount = {0}  {1}'.format(threading.activeCount(), (mgrobj is not None)))
    sys.exit(0)


def child_TERM_handler(signum, frame):
    if (mgrobj) and (getattr(mgrobj,"TERM_handler")):
        getattr(mgrobj, "TERM_handler")(signum)
    sys.exit(0)


args_cfg = {
    "direct" : {
        "tovalue" : (lambda: True),
        "switch": True
        },
    "interactive" : {
        "tovalue" : (lambda: True),
        "switch": True
        },
    }



class MultiProcMgrBase(object):
    def __init__(self, processname, *args, **kwargs):
        if (not getattr(self, "main",None)):
            raise NotImplementedError("class must define a main method")
        self._name = processname
        self._ischild = os.environ.get("MULTIPROC_SPAWNED", False)
        self._childp = None
        self._stayalive = True
        self._respawn = kwargs.get("norespawn", True)
        self._nospawn_clean = True
        self._min_proc_run = 6
        self._pidfile = os.path.join(kwargs.get("piddir", "/var/run/settv"),  ".".join([self._name, "pid"]))
        self._term_signal_sent = False
        self._childterm_wait = kwargs.get("childtermwait", 10)
        self._child_killonexit = kwargs.get("childkillonexit", True)
        self._options = {}

    def _parse_parent_args(self):
        (aoptions, otherargs) = args_parser(sys.argv[1:], args_cfg)
        self._options = aoptions.copy()
        self._base_child_args = otherargs[:]

    def start(self):
        if (not getattr(self, "_log", None)):
            self._log = logging.getLogger("__main__")
        if (not self._ischild):
            self._parse_parent_args()
        if (not self._ischild) and (not self._options["direct"]):
            if (not self._options['interactive']):
                self._daemonize()
            else:
                self._spawner()
                self._watchloop(None)
        else:
            self._init_child()
            self.main()

    def _spawner(self):
        if (self._childp is None):
            self._childp = [ None ]*self.childcount()
        self._log.info("Spawning {0} child processes".format(self.childcount()))
        for chidx in xrange(self.childcount()):
            childargs = self.childargs(chidx)
            childargs.extend(self._base_child_args[:])
            self._spawnone(chidx, childargs)
            time.sleep(1.0)

    def _spawnone(self, chidx, childargs):
        pyexecfile = os.path.basename(self._get_execfile_path())
        childenv = os.environ.copy()
        childenv["MULTIPROC_SPAWNED"] = "1"
        proc = subprocess32.Popen([sys.executable, pyexecfile] + childargs, stdin=subprocess32.PIPE, env=childenv)
        self._childp[chidx] = dict(proc=proc, retval=None, alive=True, launcht=time.time(), spawn_args=childargs)
        self._log.info("Launched {0} child process [{1}]".format(chidx, proc.pid))

    def _reap(self):
        dead_pool = []
        for (cidx,child) in enumerate(self._childp):
            if (child['retval'] is None):
                retval = child['proc'].poll()
                if (retval is not None):
                    child['retval'] = retval
                    self._log.warn('[%d] child terminated (%s)' % (child['proc'].pid, retval))
                    dead_pool.append(child)
                    self._recoverchild(retval, cidx)
        return dead_pool
        
    def _recoverchild(self, retval, chidx):
        self._log.info("Recoverchild: stayalive={0}  respawn={1} exitval={2}".format(self._stayalive, self._respawn, retval))
        if (not self._stayalive) or (not self._respawn): return
        if (retval == 0) and (self._nospawn_clean): return
        child = self._childp[chidx]
        tproc = time.time() - child['launcht']
        if (tproc < self._min_proc_run): return
        self._log.info("Respawning child: IDX={0}".format(chidx))
        self._spawnone(chidx, child['spawn_args'])
                        

    def _watchloop(self, timeout):
        self._log.info("Inside Watchloop with timeout={0}".format((timeout is not None) and timeout or "_"))
        tstart = time.time()
        while (self._stayalive) and ((timeout is None) or ((time.time() - tstart)<timeout)):
            self._reap()
            time.sleep(1)
            alive_cnt = sum((child['retval'] is None for child in self._childp))
            self._stayalive &= (alive_cnt != 0)
        self._reap()
        alive_cnt = sum((child['retval'] is None for child in self._childp))
        self._log.warn("Parent exiting watch loop  child procs running={0}".format(alive_cnt))


    def _daemonize(self):
        global mgrobj
        self.predaemon()
        pyexecfile = self._get_execfile_path()
        working_dir = os.path.dirname(os.path.realpath(pyexecfile))
        dcontext = daemon.DaemonContext(working_directory=working_dir,
                                        pidfile=TimeoutPIDLockFile(self._pidfile))
        mgrobj = self
        dcontext.signal_map = {
            signal.SIGTERM : daemon_TERM_handler
            }
        with dcontext:
            os.setsid()
            self.postdaemon()
            self._spawner()
            self._watchloop(None)

    def predaemon(self):
        pass

    def postdaemon(self):
        pass

    def childcount(self):
        return 1

    def childargs(self, chidx):
        return []

    def _get_execfile_path(self):
        return os.path.abspath(inspect.getfile(self.__class__))

    def _daemon_TERM_handler(self, signum):
        if not self._term_signal_sent:
            self._respawn = False
            self._log.info("parent daemon TERM handler")
            self._term_signal_sent = True
            self._log.warn('Sending TERM to children')
            os.killpg(0, signal.SIGTERM)
        else:
            self._log.warn('re-entry after duplicate TERM')

        waittime = self._childterm_wait + len(self._childp)*0.5
        self._watchloop(waittime)
        alive_cnt = sum((child['retval'] is None for child in self._childp))
        if (self._child_killonexit) and (alive_cnt > 0):
            self._log.warn("{0} children alive, waiting {1} secs before KILL sig".format(alive_cnt, self._childterm_wait))
            self._watchloop(self._childterm_wait)
            alive_cnt = sum((child['retval'] is None for child in self._childp))
            if (alive_cnt > 0):
                self._log.warn("Final attempt at shutdown --- Sending KILL signal to process group")
                os.killpg(0, signal.SIGKILL)
        
        self._log.warn("About to exit main daemon with {0} child alive".format(alive_cnt))
        sys.exit(0)
        return

    def _init_child(self):
        global mgrobj
        mgrobj = self
        if (getattr(self, "TERM_handler", None)):
            signal.signal(signal.SIGTERM, child_TERM_handler)
        if (getattr(self, "HUP_handler", None)):
            signal.signal(signal.SIGHUP, self.HUP_handler)
            



# if __name__ == "__main__":
#     import random
#     import os

        
#     class Tester(MultiProcMgrBase):
#         def __init__(self, *args, **kwargs):
#             super(Tester, self).__init__(args, kwargs)
#             self._totl = random.randint(10,20)

#         def childcount(self):
#             return 10
        
#         def childargs(self, child_idx):
#             return [ "--foo={0:d}".format(random.randint(10000,11000)), "--catherine", "--idx={0}".format(child_idx) ]

#         def main(self):
#             t0 = time.time()
#             while ((time.time() - t0) < self._totl):
#                 time.sleep(1.0)
#             return None

#     logging.basicConfig(level=logging.INFO)
#     t = Tester("Food")
#     t.start()
#     sys.exit(0)
    
    


        
