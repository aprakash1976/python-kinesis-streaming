import sys
import os
import imp
import inspect
import asyncio
import logging
import socket
import inspect
import json
import copy
import time
import Queue
import collections
import boto
import signal
import re
from plugin import PluginManager
from kinesisreader import KinesisReader
import daemon
from daemon.pidfile import TimeoutPIDLockFile
from affine.log import configure_logging, affine_handler
import affine.config
from mpkinesismgr import MPShardRangeMgr
from streaming.util import args_parser

#_RANGERBEADS_PID_FILE = '/tmp/rangerbeads.pid'


def parse_ranges(range_str, unique=False):
    """
    parse range string that allow comma separated integers and/or hyphen descriptons
    i.e   1,3,8,9-13,55
    returns  [1,3,8,9,10,11,12,13,55]
    """
    vints = []
    for csi in range_str.split(","):
        mo = re.match('(\d+)\-(\d+)', csi)
        if (mo):
            vints.extend([x for x in xrange(int(mo.group(1)), int(mo.group(2))+1)])
        elif (re.match('\d+', csi)):
            vints.append(int(csi))
    if (unique):
        sints = set(vints)
        vints = list(sints)
    return vints

def collapse_into_range(vi, string=False):
    ji = sorted(vi)
    pk = [[ji[0], ji[0]]]
    for mi in ji[1:]:
        if (mi != (pk[-1][1] + 1)):
            pk.append([mi, mi])
        else:
            pk[-1][1] = mi
    if (not string):
        return pk
    s = ",".join((((lo==hi) and str(lo) or "%d-%d" % (lo,hi)) for (lo,hi) in pk))
    return s



@asyncio.coroutine
def readerdispatcher(reader, pmgr):
    xlog = logging.getLogger("__main__")
    reader.start()
    xlog.warn('Reader started')
    while (True):
        lastt = time.time()
        while ((time.time() - lastt) < 0.1):
            try:
                evt = reader.getnb()
            except Queue.Empty:
                break
            else:
                xlog.debug('Dispatch to [{0}] plugins'.format(len(pmgr._plugins)))
                pmgr.dispatch(evt)
        yield asyncio.sleep(1)


class RangerBeadsMgr(MPShardRangeMgr):
    _args_cfg = {
        "debug": {
            "switch" : True,
            "shortcut" : "V"
            },
        "plugin_directory": {
            "shortcut": "D"
            },
        "shard_indices": {
            "tovalue": parse_ranges,
            "shortcut": "S"
            },
        "plugins": {
            "shortcut": "P"
            },
        }
    def __init__(self, *args, **kwargs):
        super(RangerBeadsMgr, self).__init__("rangerbeads", *args, **kwargs)
        self._kreader = None
        self._plugin_mgr = None
        self._plugin_dir = None
        self._plugins = []
        self._loop = kwargs.get('loop', asyncio.get_event_loop())

    def _scan_plugin_dir(self):
        plist = os.listdir(self._plugin_dir)
        for pfile in plist:
            if (pfile.endswith('.py')):
                ls = os.lstat(os.path.join(self._plugin_dir, pfile))
                gfile = os.path.realpath(os.path.join(self._plugin_dir, pfile))
                self._plugins.append((gfile, ls.st_mtime))

    def add_plugins(self, plugins):
        plist = []
        for pfile in plugins.split(','):
            if (not os.path.isabs(pfile)):
                pfile = os.path.join(os.getcwd(), pfile)
                if (not os.path.exists(pfile)) or (not pfile.endswith('.py')):
                    sys.stderr.write("plugin file {0} does not exist or invalid\n".format(pfile))
                    sys.exit(1)
            fs = os.lstat(pfile)
            plist.append((pfile, int(fs.st_mtime)))
        return plist

    def TERM_handler(self, signum):
        self._log.warn('Trapped TERM... shutting down threads {0}'.format((self._kreader is not None)))
        if (self._kreader):
            self._kreader.shutdown()
            while (self._kreader.alive()):
                time.sleep(0.1)
        if (self._plugin_mgr):
            self._plugin_mgr.shutdown()
            while (self._plugin_mgr.alive()):
                self._log.warn('Waiting for plugin mgr to stop')
                time.sleep(0.1)
        self._log.warn('Main thread exit')
        sys.exit(0)

    def HUP_handler(self, signum, frame):
        self._log.warn('Caught HUP, scanning {0} for {1}'.format(self._plugin_dir, self._plugin_mgr))
        # this is incorrect -- need to quiesce modules/threads first
#        if (self._plugin_mgr) and  (self._plugin_dir):
#            plugs = scan_plugin_dir(self._plugin_dir)
#            for (pluginfile, lmodtime) in plugs:
#                self._log.info('Registering plugin file: {0}'.format(pluginfile))
#                self._plugin_mgr.register_by_filepath(pluginfile, lastupdate=lmodtime)
        
    def main(self):
        (aoptions, otherargs) = args_parser(sys.argv[1:], self._args_cfg)
        self._opts = aoptions.copy()
        source_stream = filter((lambda x: not x.startswith("-")), otherargs)[0]
        suffix = collapse_into_range(self._opts["shard_indices"], string=True)
        configure_logging("{0}_{1}.log".format(self._name, suffix))
        alog = logging.getLogger('asyncio')
        alog.setLevel(logging.WARN)
        alog.addHandler(affine_handler())
        self._log = logging.getLogger("__main__")
        self._log.setLevel(logging.INFO)
        self._log.info("Starting rangerbeads {0}".format(suffix))
        if ('plugin_directory' in self._opts):
            if (not os.path.isdir(self._opts["plugin_directory"])):
                self._log.warn("plugin directory {0} does not exist".format(self._opts["plugin_directory"]))
                sys.exit(1)
            self._plugin_dir = self._opts['plugin_directory']
            self._scan_plugin_dir()
        self._opts['librato_source'] = suffix
        self._opts['ranger_instance'] = suffix
        self._plugin_mgr = PluginManager(opts=self._opts, logger=self._log)
        for (pluginfile, modtime) in self._plugins:
            self._log.info('Registering plugin file: {0}'.format(pluginfile))
            self._plugin_mgr.register_by_filepath(pluginfile)
        self._kreader = KinesisReader(source_stream, self._opts['shard_indices'], logger=self._log)
        readdisp = readerdispatcher(self._kreader, self._plugin_mgr)
        self._loop.run_until_complete(readdisp)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pass
    

if __name__ == "__main__":
    rmgr = RangerBeadsMgr()
    rmgr.start()
