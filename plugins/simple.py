import logging

#
#  when the module is imported on startup, this
#  array specifies which plugin (function/callback) 
#  to invoke
#
plugins = ["count"]



class PluginSimple(object):
    def __init__(self, *args, **kwargs):
        self._count = 0
        self._log = logging.getLogger("__main__")
    
    def count(self, vdata, **kwargs):
        for evt in vdata:
            self._count += 1
            if ((self._count % 10000) == 0):
                self._log.info("EventCount: {0}".format(self._count))

psimple = PluginSimple()


def count(vdata, **kwargs):
    if (not isinstance(vdata, list)): return
    psimple.count(vdata, **kwargs)



