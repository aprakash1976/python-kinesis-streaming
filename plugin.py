import logging
import sys
import imp
import inspect
import json
import os
from threading import Thread
import concurrent.futures
import Queue
import time
from streaming.util import IntervalDispatcher, import_fullpath, find_dotted_module


class Plugin(Thread):
    def __init__(self, cb, *args, **kwargs):
        super(Plugin, self).__init__()
        self.daemon = True
        self._cb = cb
        self._name = kwargs.get('name', '(unknown)')
        self._enabled = True
        self._running = False
        self._stop = False
        self._fifoq = Queue.Queue()
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._opts = kwargs.get('opts', {})
        self._processed = 0

    def __str__(self):
        return '{0}<{1}>{2}[{3}]({7}) Enabled:{4} Running:{5} {6}'.format(
            self.__class__.__name__, id(self), self._name, self._fifoq.qsize(), 
            self._enabled, self._running, self._cb, self._processed)

    def run(self):
        if (not self._enabled): return
        self._running = True
        self._plog.info('{0} entered run state  opts={1}'.format(str(self), ','.join((k+":"+str(v) for (k,v) in self._opts.items()))))
        while (not self._stop):
            data = self._fifoq.get(True)
            if (self._enabled) and (data):
                try:
                    self._cb(data, opts=self._opts)
                except:
                    self._plog.exception("trapped exception on plugin: {0}".format(self._name))
                    self._plog.warn("Disabling plugin: {0}".format(self._name))
                    self._enabled = False
                    self._stop = True
                else:
                    self._processed += 1
        self._running = False
        self._plog.warn('plugin stopped... exiting')

    def dispatch(self, data):
        if (self._enabled) and (self._running):
            self._fifoq.put(data)

    def stop(self):
        self._plog.warn('{0} stopping   Qsize: [{1}]'.format(str(self), self._fifoq.qsize()))
        self._stop = True
        self._fifoq.put(None)

    def running(self):
        return self._running
        


class PluginManager(object):
    def __init__(self, *args, **kwargs):
        self._static_modules = set(sys.modules.keys())
        self._plog = kwargs.get('logger', logging.getLogger('__main__'))
        self._plugins = {}
        self._mfilemtime = {}
        self._opts = kwargs.get('opts', {})

    def dispatch(self, data):
        for name, plugin in self._plugins.items():
            self._plog.debug('PluginManager.dispatch {0}  {1}'.format(name, str(plugin)))
            plugin.dispatch(data)

    def shutdown(self):
        self._plog.warn('pluginmgr shutting down')
        for name, plugin in self._plugins.items():
            plugin.stop()

    def alive(self):
        if (len(self._plugins) == 0):
            return False
        else:
            return all((plugin.running() for plugin in self._plugins.values()))

    def register_by_modulename(self, modulename):
        '''
        register a plugin via module name
        '''
        modpath = find_dotted_module(modulename)
        if (not modpath):
            self._plog.warn('Cannot find module: {0}'.format(modulename))
            return
        umodule = self.register_by_filepath(modpath)
        if (umodule is None):
            self._plog.warn('Failed to import plugin module {0} {1}'.format(modulename, modpath))
        return

    def register_by_filepath(self, module_path, plugins=None, lastupdate=None):
        '''
        register a plugin from module absolute-pathname
        '''
        if (module_path in self._mfilemtime):
            if (self._mfilemtime[module_path]['mtime'] < lastupdate):
                self._plog.warn('Reloading existing module: {0}'.format(module_path))
            else:
                self._plog.warn('Skipped reload existing module: {0} (unchanged)'.format(module_path))
                return self._mfilemtime[module_path]['module']
        tnow = int(time.time())
        umodule = import_fullpath(module_path)
        self._mfilemtime[module_path] = dict(module=umodule, mtime=(lastupdate or tnow))
        if (not umodule): return None
        self._plog.info('Import module {0}'.format(umodule.__file__))
        if (plugins is None):
            plugins = getattr(umodule, 'plugins', None)
        if (not plugins) or (not isinstance(plugins, list)): return None
        self._plog.info('Registering plugins: {0}'.format(plugins))
        for callback in plugins:
            cb = getattr(umodule, callback, None)
            if (not cb):
                self._plog.warn('incorrect plugins descriptor callback {0} not in {1}'.format(callback, umodule.__name__))
                return None
            try:
                cb(dict(hello='world'))
            except:
                self._plog.warn("failed failed to invoke plugin {0} @ {1}",format(umodule.__name__, callback))
                return None
            plugin_key = umodule.__file__ + ':' + callback
            self._plugins[plugin_key] = Plugin(cb, name=plugin_key, opts=self._opts)
            self._plugins[plugin_key].start()
        self._plog.info('Plugin count: {0}'.format(len(self._plugins)))
        return umodule



class PeriodicPluginBase(object):
    def __init__(self, period, *args, **kwargs):
        self._tdispatcher = IntervalDispatcher(period, self, 'periodic', daemon=True, **kwargs)
        self._started = False
        self._opts = {}

    def periodic(self):
        pass

    def start(self, **kwargs):
        if(not self._started):
            self._opts = kwargs.get("opts", {})
            self._tdispatcher.dispatch()
            self._started = True

