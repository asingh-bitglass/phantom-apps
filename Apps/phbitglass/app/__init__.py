"""
(C) Copyright Bitglass Inc. 2021. All Rights Reserved.
Author: eng@bitglass.com
"""
__author__ = 'Bitglass'


class Logger(object):
    """ For redirecting logging output to proprietory platform APIs (such as QRadar).
        Default ctor is equivalent to logging not defined as the case is when on some platforms
        one needs to know the data path from the config first before can initialize logging.
        This allows for using app.logger.xyz() across the app in a portable way across platforms.
        By default the standard python 'logging' module is used whenever possible.
    """

    def debug(self, msg):
        self.log(msg, level='debug')

    def info(self, msg):
        self.log(msg, level='info')

    def warning(self, msg):
        self.log(msg, level='warn')

    def error(self, msg):
        self.log(msg, level='error')

    def nop(self, msg):
        pass

    def __bool__(self):
        return bool(self.conf)

    # TODO Remove Python 2 crutch
    def __nonzero__(self):
        return self.__bool__()

    def __init__(self, conf=None, log=None, set_log_level=None):
        self.conf = conf
        self.log = log
        if conf and log and set_log_level:
            if 'error' in conf.logging_level.lower():
                set_log_level('error')
                self.debug = self.nop
                self.info = self.nop
                self.warning = self.nop
            elif 'warn' in conf.logging_level.lower():
                set_log_level('warn')
                self.debug = self.nop
                self.info = self.nop
            elif 'info' in conf.logging_level.lower():
                set_log_level('info')
                self.info = self.nop
        else:
            self.debug = self.nop
            self.info = self.nop
            self.warning = self.nop
            self.error = self.nop


# Can't initialize here b/c it's data path dependent (different for different platforms)
logger = Logger()


# Uncomment for the merged module support
# class App:
#     def __init__(self, logger):
#         self.logger = logger
#
#
# app = App(logger)


try:
    # Export for gunicorn
    from app.flaskinit import app   # noqa

    # This will be initialized in Initialize() called from the UI code from startWorkerThread()
    logger = logger

    from app.flaskinit import log, set_log_level   # noqa
    from app.config import startConf

    # Re-direct logging to QRadar lib and override the logger
    logger = Logger(startConf, log, set_log_level)

# Not ImportError, to support arbitrary failing of cli integrations from the flat package, just in case
except Exception:

    logger = logger
