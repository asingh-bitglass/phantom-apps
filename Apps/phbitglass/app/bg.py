"""
(C) Copyright Bitglass Inc. 2021. All Rights Reserved.
Author: eng@bitglass.com
"""

import os
import json

from six import PY2
from six.moves import socketserver


if PY2:
    import urllib2 as urllib
    from urllib2 import HTTPError
else:
    from urllib.error import HTTPError
    import urllib.request as urllib
    # TODO ?? For some weird reason, the requests session is closed on first reference
    # if imported here globally (to move the failure earlier)
    # import requests_oauth2


import base64

import time
import copy
from threading import Thread, Condition
from datetime import datetime, timedelta
# from threading import get_ident


import app
from app.env import UpdateDataPath, datapath, loggingpath
from app.config import byteify, open_atomic, setPythonLoggingLevel, setPythonLogging
from app.configForward import ConfigForward, log_types
from app.logevent import pushLog


from app.cli import main


conf = None
lastLogFile = None


def ingestLogEvent(ctx, d, address, logTime):
    if ctx and ctx.ctx is not None:
        ctx.ctx.bgPushLogEvent(d, address, logTime)

    # TODO Prevent recursion sending to itself with syslog socket
    return pushLog(d, address, logTime)


def flushLogEvents(ctx):
    if ctx and ctx.ctx is not None:
        ctx.ctx.bgFlushLogEvents()


def Initialize(ctx, datapath=datapath, skipArgs=False, _logger=None, _conf=None):
    global conf
    global lastLogFile

    # Monkey patch env.datapath first with the value from the command line to read bg json configs
    updatepath = False
    if datapath:
        updatepath = UpdateDataPath(datapath)

    if not app.logger and not _logger:
        app.logger = setPythonLogging(None, datapath)
        app.logger.info('~~~ Running in CLI mode (Python logging set) ~~~')

    if not datapath:
        # Put in the same directory as the logging file (the latter would be well-defined, without uuids)
        datapath = os.path.split(loggingpath)[0] + os.sep
        updatepath = UpdateDataPath(datapath)
        if updatepath:
            conf = None

    if not conf or updatepath:
        if not _conf:
            conf = ConfigForward()
            # Be sure to update the logging level once the config is loaded
            setPythonLoggingLevel(app.logger, conf)

            if not skipArgs or conf._isEnabled('debug'):
                # Parse and apply command line options. Always process for a local dev run ('debug'), it's compatible
                conf._applyOptionsOnce()
        else:
            conf = _conf

    # Override the configuration
    if ctx and ctx.ctx is not None:
        # Override the config settings and disable daemon mode for explicit cli context
        ctx.ctx.bgLoadConfig(conf)
        conf._isDaemon = False
    conf._calculateOptions()

    if not lastLogFile or updatepath:
        cnf = _conf or conf

        # For Splunk app upgrade, manually 'cp lastlog.json ../local/' before upgrading to ingest incrementally
        # This is because it was saved in default/ in the previous version 1.0.8 and default/ is yanked during upgrade
        folder = cnf._folder
        if (os.path.sep + 'default') in cnf._folder:
            folder = os.path.join(folder, '..', 'local', '')

        lastLogFile = LastLog(os.path.join(folder, 'lastlog'))

    return conf


class SyslogUDPHandler(socketserver.BaseRequestHandler):
    kwargs = None
    callback = None

    @classmethod
    def start(cls, callback, host, port=514, poll_interval=0.5, **kwargs):
        cls.kwargs = kwargs
        cls.callback = callback
        # TODO Test exception propagation to the main thread (due to a bad host?), may need handling
        try:
            server = socketserver.UDPServer((host, port), cls)
            server.serve_forever(poll_interval=poll_interval)
        except (IOError, SystemExit):
            raise
        except KeyboardInterrupt:
            app.logger.info("Crtl+C Pressed. Shutting down.")

    def handle(self):
        # logger = self.kwargs['logger']
        data = bytes.decode(self.request[0].strip())

        # Strip the string and convert to json
        try:
            conf = self.kwargs['conf']
            condition = self.kwargs['condition']

            s = u'%s :{' % conf.customer.lower()
            start = data.find(s) + len(s) - 1
            end = data.rfind(u'}') + 1
            logData = json.loads(u'{"response":{"data":[' + data[start:end] + u']}}')

            with conf._lock(condition, notify=False):
                transferLogs(None, logTypes=None, logData=logData, npt=None, **self.kwargs)

        except Exception as ex:
            app.logger.warning('{0}\n - Discarded bad event message in syslog stream:\n"{1}"\n- from sender {2}'.format(
                str(ex),
                data,
                self.client_address[0])
            )
            return


TIME_FORMAT_URL = '%Y-%m-%dT%H:%M:%SZ'
TIME_FORMAT_LOG = '%d %b %Y %H:%M:%S'
TIME_FORMAT_ISO = '%Y-%m-%dT%H:%M:%S.%fZ'


def strptime(s):
    # For more reliable (but slower) datetime parsing (non-English locales etc.) use:
    # pip install python-dateutil
    # from dateutil import parser
    # parser.parse("Aug 28 1999 12:00AM")  # datetime.datetime(1999, 8, 28, 0, 0)
    # '06 Nov 2018 08:15:10'

    d = datetime.strptime(s, TIME_FORMAT_LOG)

    return d


class LastLog:
    def __init__(self, fname, shared=None, logtype=''):
        self.fname = '{0}-{1}.json'.format(fname, logtype) if shared else '{0}.json'.format(fname)
        self.shared = shared
        self.log = {}
        self.logtype = logtype
        self.subLogs = {}
        try:
            with open(self.fname, 'r') as f:
                self.log = byteify(json.load(f))

                if self.shared is None:
                    # This is a shared (old) file. Convert to the new format if needed
                    for lt in log_types:
                        if self.get(logtype=lt) and isinstance(self.log[lt], str):
                            self.log[lt] = json.loads(self.log[lt])
        except Exception as ex:
            app.logger.info('{0}\n - Last log file {1} not found'.format(str(ex), self.fname))
            lastLog = {}
            if self.shared:
                lastLog[logtype] = self.shared.log[logtype]
            else:
                for lt in log_types:
                    lastLog[lt] = {}
            self.log = lastLog

        # Create children one per log type unless sharing the same file for all
        if self.shared is None and logtype != 'share':
            for lt in log_types:
                self.subLogs[lt] = LastLog(fname, self, lt)

    def dump(self):
        try:
            with open_atomic(self.fname, 'w') as f:
                json.dump(self.log, f, indent=4, sort_keys=True)
        except Exception as ex:
            app.logger.error('Could not save last log event across app sessions: %s' % ex)

    def get(self, field=None, logtype=None):
        if logtype is None:
            logtype = self.logtype
        else:
            if logtype in self.subLogs:
                return self.subLogs[logtype].get(field)

        if field:
            ll = self.log[logtype]
            # Handle the old format to be forward compatible across upgrade
            res = json.loads(ll) if isinstance(ll, str) else ll
            return res[field] if field in res else None

        return logtype in self.log and self.log[logtype] != {}

    def getHistoricLogTypeList(self):
        return [lt for lt in log_types if self.get(logtype=lt)]

    def update(self, ll, logtype=None):
        if logtype is None:
            logtype = self.logtype
        else:
            if logtype in self.subLogs:
                return self.subLogs[logtype].update(ll)

        if ll:
            # Add extra fields for diagnostics. Should not lag event log timestamp more than by the API polling interval
            ll[u'_ingestedtime'] = datetime.utcnow().strftime(TIME_FORMAT_LOG)
            self.log[logtype] = ll
            self.dump()
        else:
            # This is a successful request with empty (exhausted) data so use the last one but handle the (corner) case
            # of error logged inbetween (coinsiding with app relaunch) by clearing the error entries if there are any
            if self.get(logtype=logtype):
                if self.get('_failedtime', logtype):
                    del self.log[logtype]['_failedtime']
                if self.get('_errormessage', logtype):
                    del self.log[logtype]['_errormessage']

        if logtype in self.log:
            return json.dumps(self.log[logtype])
        else:
            return ''

    def updateError(self, conf, errormsg, resettime, logtype=None):
        if logtype is None:
            logtype = self.logtype
        else:
            if logtype in self.subLogs:
                return self.subLogs[logtype].updateError(conf, errormsg, resettime)

        if not self.get(logtype=logtype):
            self.log[logtype] = {}

        # Handle the corner case of never having a successful message ever yet so add the missing time
        # field to make the recovery possible (the data reading code defaults to 'now' in such case to
        # play it safe and avoid the possible data duplication but this leads to never getting
        # good messages unless by fluke of hitting upon a very recent message). Assume the initial
        # data period just the same as the reading code does when starting up.
        # This provides for an alternative hack to reset the log type: edit the file and rename the 'time' field.
        if not self.get('time', logtype) or resettime:
            if self.get('time', logtype):
                # Backup the original 'time' field if present by renaming it first
                self.log[logtype]['_time'] = self.log[logtype]['time']
            t = datetime.utcnow() + timedelta(seconds=-1 * conf.log_initial) if not resettime else resettime
            self.log[logtype]['time'] = t.strftime(TIME_FORMAT_LOG)

        # Update with failure timestamp and message, keep last ingested success timestamp
        self.log[logtype]['_failedtime'] = datetime.utcnow().strftime(TIME_FORMAT_LOG)
        self.log[logtype]['_errormessage'] = str(errormsg)
        self.dump()

    def clobber(self, resettime=None, logtype=None):
        # Clobbering the children with empty json {} is the simplest and most robust way to go.
        # Actually deleting the file would not be enough as the old format file persist, so
        # implementing it correctly would need atomically updating more than one file which would
        # complicate the code immensely and for Splunk, even the use of temp files is a potential
        # certification problem (although bogus one IMO)
        if logtype is None:
            logtype = self.logtype
        else:
            if logtype in self.subLogs:
                return self.subLogs[logtype].clobber(resettime)

        # Just write bare braces (without adding the log type) keeping it simpler
        # self.log[logtype] = {}
        self.log = {}

        # The softer option of rolling back the time (provided by the UI already for flexibility)
        # TODO Ignore for the simplicity sake until the UI provides the actual reset time
        # if resettime:
        #     self.log[logtype] = {}
        #     self.log[logtype]['time'] = resettime.strftime(TIME_FORMAT_LOG)

        self.dump()


def getAPIToken(logData, conf, logType):
    if not conf.useNextPageToken:
        return None

    try:
        token = logData['nextpagetoken']
        d = json.loads(base64.b64decode(token))
    except Exception as ex:
        app.logger.warning('Invalid token returned: %s %s' % (token, ex))
        return None

    # TODO Swap the condition for compatibility if the new logtypes introduced use the same fields as in swqweb*
    if logType != u'swgweb' and logType != u'swgwebdlp':
        # Older log types
        if 'log_id' not in d:
            app.logger.warning('No "log_id" encoded in returned token: %s' % token)
            return None

        if 'datetime' not in d:
            app.logger.warning('No "datetime" encoded in returned token: %s' % token)
            return None
    else:
        # Newer log types
        if 'start_time' not in d:
            app.logger.warning('No "start_time" encoded in returned token: %s' % token)
            return None

        if 'end_time' not in d:
            app.logger.warning('No "end_time" encoded in returned token: %s' % token)
            return None

        if 'page' not in d:
            app.logger.warning('No "page" encoded in returned token: %s' % token)
            return None

    return token


SKIPPED_REQUEST_ERROR = 'UNAUTHORiZED'


def RestParamsLogs(_, host, api_ver, logType, npt, dtime):
    url = ('https://portal.' + host) if host else ''
    endpoint = '/api/bitglassapi/logs'

    # Adjust the version upwards for new log types as necessary
    if logType == u'swgweb' or logType == u'swgwebdlp':
        # TODO Make sure it's lower before overriding
        api_ver = '1.1.0'

    if npt is None:
        urlTime = dtime.strftime(TIME_FORMAT_URL)
        dataParams = '/?cv={0}&responseformat=json&type={1}&startdate={2}'.format(api_ver, str(logType), urlTime)
    else:
        dataParams = '/?cv={0}&responseformat=json&type={1}&nextpagetoken={2}'.format(api_ver, str(logType), npt)

    return (url, endpoint, dataParams)


def RestParamsConfig(_, host, api_ver, type_, action):
    url = ('https://portal.' + host) if host else ''

    # This is a POST, version is not a proper param, unlike in logs (?? for some reason)
    endpoint = '/api/bitglassapi/config/v{0}/?type={1}&action={2}'.format(api_ver, type_, action)
    return (url, endpoint)


def restCall(_,
             url, endpoint, dataParams,
             auth_token,
             proxies=None,
             method=None,
             verify=True,
             username=None,
             password=None):
    if dataParams is None:
        dataParams = ''

    if auth_token is None or auth_token == '':
        auth_type = 'Basic'

        # Must have creds supplied for basic
        if (username is None or username == '' or
                password is None or password == ''):
            # Emulate an http error instead of calling with empty password (when the form initially loads)
            # to avoid counting against API count quota
            raise HTTPError(url + endpoint, 401, SKIPPED_REQUEST_ERROR, {}, None)

        if PY2:
            auth_token = base64.b64encode(username + ':' + password)
        else:
            auth_token = base64.b64encode((username + ':' + password).encode('utf-8'))
            auth_token = auth_token.decode('utf-8')
    else:
        auth_type = 'Bearer'

    try:
        # This check is done earlier for PY3 to fail before run time
        # if PY2:
        import requests_oauth2
        haveOAuth2 = True
    except ImportError as ex:
        app.logger.warning('{0}\n - Defaulting to the legacy urllib module'.format(str(ex)))
        haveOAuth2 = False

    # Use requests by default if available
    # Note: requests-oauth2 is not installed on QRadar by default
    r = None
    if (method is None or method == 'requests') and haveOAuth2:
        import requests
        from requests.auth import HTTPBasicAuth

        # The authentication header is added below
        headers = {'Content-Type': 'application/json'}

        d = {}
        with requests.Session() as s:
            if auth_type == 'Basic':
                s.auth = HTTPBasicAuth(username, password)
            else:
                s.auth = requests_oauth2.OAuth2BearerToken(auth_token)

            if proxies is not None and len(proxies) > 0:
                s.proxies = proxies

            if isinstance(dataParams, dict):
                # Assume json
                r = s.post(url + endpoint, headers=headers, verify=verify, json=dataParams)
            else:
                # TODO Inject failures (including the initial failure) for testing: raise Exception('test')
                r = s.get(url + endpoint + dataParams, headers=headers, verify=verify)

            r.raise_for_status()
            d = r.json()
    else:
        headers = {'Content-Type': 'application/json', 'Authorization': auth_type + ' ' + auth_token}

        if isinstance(dataParams, dict):
            # Assume json
            req = urllib.Request(url + endpoint, json.dumps(dataParams), headers, unverifiable=not verify)
            if PY2:
                req = urllib.Request(url + endpoint, json.dumps(dataParams), headers, unverifiable=not verify)
            else:
                req = urllib.Request(url + endpoint, json.dumps(dataParams).encode('utf-8'), headers, unverifiable=not verify)
        else:
            req = urllib.Request(url + endpoint + dataParams, None, headers, unverifiable=not verify)

        if proxies is not None and len(proxies) > 0:
            # TODO ?? Do it once at init time unless this option ends up exposed in the UI
            opener = urllib.build_opener(urllib.ProxyHandler(proxies))
            urllib.install_opener(opener)

        # TODO Security scan medium. Remove urllib fallback use when QRadar moves to Python 3
        resp = urllib.urlopen(req)  # nosec: <explanation>The url is validated to be https</explanation>
        respTxt = resp.read()
        d = json.loads(respTxt)

    return d, r


def RestCall(_, endpoint, dataParams):
    return restCall(
        _,
        'https://portal.' + conf.host,
        endpoint,
        dataParams,
        conf._auth_token.pswd,
        conf.proxies,
        conf.method,
        conf.verify,
        conf._username,
        conf._password.pswd
    )


def drainLogEvents(ctx, dtime, conf, logType, logData=None, nextPageToken=None):

    status = conf.status[logType]
    r = None

    isSyslog = (logData is not None)

    if conf._reset_time and not isSyslog:
        # Must be validated by the app if it's the actual time datetime.datetime
        # TODO Can switch to using format TIME_FORMAT_ISO for conf._reset_time here and in the UI
        dtime = datetime.utcnow() + timedelta(seconds=-1 * conf.log_initial)
        nextPageToken = None

        # Override the last log data with the new 'time' field
        if conf.hardreset:
            # This should be the default.
            # Clobber all the data in the file as the last resort! This is the important fool-proof method for
            # ultimate UI control on the cloud if some bug is suspected to hold new messages etc.
            app.logger.warning('Hard-reset initiated due to user request. The data will resume when new messages get available.')
            lastLogFile.clobber(dtime, logtype=logType)
        else:
            # The soft reset mode is essential for testing by keeping the state around. It's used for auto-reset as well
            app.logger.warning('Soft-reset initiated due to user request. The data will resume when new messages get available.')
            lastLogFile.updateError(conf, 'Soft-reset initiated due to user request. Waiting for the new data becoming available starting from (see the \'time\' field below)', dtime, logType)

    logTime = dtime

    try:
        i = 0
        drained = False
        while not drained:
            if isSyslog:
                drained = True
            else:
                if i > 0:
                    # This is a crude way to control max event rate for Splunk / QRadar etc. as required
                    # without adding another thread and a queue which is a design over-kill
                    time.sleep(1.0 / conf._max_request_rate)

                if conf.host == conf._default.host:
                    # Avoid the overhead of invalid request even if there is no traffic generated
                    raise HTTPError(conf.host, -2, SKIPPED_REQUEST_ERROR, {}, None)

                # TODO If there is a hint from API that all data is drained can save the
                # split second sleep and the extra request
                url, endpoint, dataParams = RestParamsLogs(None,
                                                           conf.host,
                                                           conf.api_ver,
                                                           logType,
                                                           nextPageToken,
                                                           logTime + timedelta(seconds=1))
                logData, r = restCall(None,
                                      url, endpoint, dataParams,
                                      conf._auth_token.pswd,
                                      conf.proxies,
                                      conf.method,
                                      conf.verify,
                                      conf._username,
                                      conf._password.pswd)
                i = i + 1

            lastLog = None
            nextPageToken = getAPIToken(logData, conf, logType)

            # Querying API data by 'time' field (not using nextpagetoken) is broken for 1.1.0 log types
            # swgweb and swgwebdlp causing overlaps. So disable the fallback path for them (no nextpagetoken)
            # No fix planned, so this workaround is a keeper
            # TODO Swap the condition for compatibility when new logtypes get introduced7
            isNewLogType = logType == u'swgweb' or logType == u'swgwebdlp'
            if nextPageToken is None and isNewLogType:
                raise ValueError('Invalid page token for swgweb* log types is not supported')

            data = logData['response']['data']
            if len(data) == 0:
                drained = True
            else:
                # Cover the case of reverse chronological order (in case of not reversing it back)
                lastLog = data[0]

                for d in data[::-1 if strptime(data[0]['time']) > strptime(data[-1]['time']) else 1]:
                    # In some new log types like swgweb the data are sorted from recent to older
                    # So let's not assume chronological order to be on the safe side..
                    tm = strptime(d['time'])

                    # Inject logtype field, it's needed by QRadar Event ID definition (defined in DSM editor)
                    if u'logtype' not in d:
                        d[u'logtype'] = logType

                    # NOTE Use logTime if QRadar has problems with decreasing time (as in swgweb and swgwebdlp)
                    ingestLogEvent(ctx, d, conf._syslogDest, tm)

                    if nextPageToken is None:
                        d[u'nextpagetoken'] = u''
                    else:
                        d[u'nextpagetoken'] = nextPageToken

                    if (tm > logTime or
                            # This is to avoid the possible +1 sec skipping data problem (if no npt)
                            not isNewLogType):
                        logTime = tm
                        lastLog = d
                        # json.dumps(d, sort_keys=False, indent=4, separators = (',', ': '))

            status.cntSuccess = status.cntSuccess + 1
            status.lastMsg = 'ok'
            status.lastLog = lastLogFile.update(lastLog, logType)

            flushLogEvents(ctx)

    except Exception as ex:
        msg = 'Polling: failed to fetch log event data "%s": %s' % (str(logType), ex)
        if SKIPPED_REQUEST_ERROR in msg:
            # No valid settings have been set yet so avoid polluting the log. IMO this is still useful for debugging
            # app.logger.debug(msg)
            pass
        else:
            app.logger.error(msg)
            r = ex
            lastLogFile.updateError(conf, r, None, logType)

        status.cntError = status.cntError + 1
        status.lastMsg = str(ex)
        status.lastLog = ''

    # NOTE  Last successful result has empty data now (drained), instead, could merge all data and return
    #       making it optional if ingestLogEvent is not set.. Without it, attaching data to result is rather useless
    status.lastRes = r
    status.lastTime = logTime

    conf.status['last'] = status

    return logTime


def transferLogs(ctx, conf, condition, dtime, logTypes=None, logData=None, npt=None):
    myConf = conf._deepcopy()
    condition.release()

    if not logTypes:
        if myConf._reset_time:
            # Pull all the log types that have ever been pulled unless the specific log type list was provided (like in Phantom).
            # Merge with the currently specified ones
            # logTypes = log_types   # All possibly supported log types
            h = lastLogFile.getHistoricLogTypeList()
            n = myConf.log_types
            logTypes = h + list(set(n) - set(h))   # Whatever have been tried from the last reset plus currently requested
        else:
            logTypes = myConf.log_types    # Only currently requested

    logTime = {}
    if logData is None:
        for lt in logTypes:
            logTime[lt] = drainLogEvents(ctx, dtime[lt], myConf, lt, logData,
                                         npt[lt] if npt is not None else None)
    else:
        # syslog source
        # Make sure nextpagetoken is disabled
        myConf.useNextPageToken = False
        lt = logData['response']['data'][0]['logtype']
        logTime[lt] = drainLogEvents(None, dtime[lt], myConf, lt, logData=logData)

    condition.acquire()
    myConf.status['updateCount'] = conf.updateCount

    # Load the latest state for the UI and reset the read-once-reset-to-default config params
    conf.status = copy.deepcopy(myConf.status)
    if conf._reset_time:
        if conf._isForeignConfigStore():
            conf.reset_fence = datetime.utcnow().isoformat()
            conf._save()
        conf._reset_time = ''

    # Increment by smallest delta to avoid repeating same entries
    # TODO Using microseconds=1 causes event duplication.. what is the minimum resolution to increment??
    #       without data loss but with guaranteed no repetitions
    if logData is None:
        if conf._isDaemon:
            condition.wait(myConf.log_interval)
        for lt in logTypes:
            dtime[lt] = logTime[lt] + timedelta(seconds=1)
    else:
        # syslog source
        dtime[lt] = logTime[lt] + timedelta(seconds=1)


def PollLogs(ctx, conf, logTypes=None, condition=Condition()):
    """
    Pump BG log events from BG API to QRadar
    """

    time.sleep(10)

    pid = os.getpid()
    # tid = get_ident()
    tid = 0
    app.logger.info('================================================================')
    app.logger.info('Polling: start polling log events.. pid=%s, tid=%s' % (pid, tid))
    app.logger.info('----------------------------------------------------------------')

    # Have to complicate things b/c the API doesn't support combining different log types
    dtime = {}
    npt = {}
    now = datetime.utcnow()
    for lt in log_types:
        # = datetime.utcnow() + timedelta(days=-1)
        dtime[lt] = now + timedelta(seconds=-1 * conf.log_initial)
        npt[lt] = None

        # Adjust to avoid the overlap with a previous run, warn on a possible gap
        # The gap is caused by either: app down time or the log source being disabled in earlier app run
        # was greater than 30 days (default of 'log_initial')
        try:
            if lastLogFile.get(logtype=lt):
                try:
                    # Could be missing due to the old file format
                    npt[lt] = lastLogFile.get('nextpagetoken', lt)
                    if npt[lt] == '':
                        npt[lt] = None
                except Exception:
                    npt[lt] = None

                d = strptime(lastLogFile.get('time', lt))
                if dtime[lt] <= d:
                    dtime[lt] = d
                else:
                    # Important! For a possible gap, discard nextpagetoken loaded from lastlog
                    # NOTE: This still has an extremely remote possibility of data duplication
                    #       (no messages over the gap period is a necessary condition then - unpopulated gap)
                    npt[lt] = None
                    app.logger.warning('Possible gap for log type %s from %s to %s' %
                                       (str(lt),
                                           d.strftime(TIME_FORMAT_LOG),
                                           dtime[lt].strftime(TIME_FORMAT_LOG))
                                       )
        except Exception as ex:
            # Bad data in lastLogFile? Treat overlap as data corruption so exclude its possibility and warn
            # Discard nextpagetoken loaded from lastlog, also see the comment just above
            npt[lt] = None

            # By just setting it to 'now' the bad data would persist without getting any new messages and
            # hence no chance to reset the data to good format unless due to a fluke of a very recent message.
            # Instead, do a soft reset
            dtime[lt] = now
            app.logger.error('Probable gap for log type %s to %s due to BAD LAST LOG DATA: %s' %
                             (str(lt),
                                 dtime[lt].strftime(TIME_FORMAT_LOG),
                                 ex)
                             )

            # Re-write the file back to the good format
            # TODO If wishing to reduce missing a lot of data in favor of some overlap,
            #      may rely on the last log file mofification time minus polling period (which one? could have changed)
            app.logger.warning('Auto-reset initiated due to bad last log data. The data will resume when new messages get available.')
            lastLogFile.updateError(conf, 'Auto-reset initiated due to bad last log data. Waiting for the new data becoming available starting from (see the \'time\' field below)', now, lt)

            # Should never end up here again unless the file gets invalidated outside the app once more

    # Assume syslog daemon
    # TODO Add a mechanism to stop to switch back to API poll mode, restart is
    # required for now (after manual config edit)
    isSyslog = ('://' not in conf.api_url and
                len(conf.api_url.split(':')) == 2)
    res = None
    if isSyslog:
        host, port = conf.api_url.split(':')
        # TODO: At least verify that sink_url is different to reduce the loop possibility sending back to itself
        SyslogUDPHandler.start(transferLogs,
                               host=int(host),
                               port=int(port),
                               conf=conf,
                               condition=condition,
                               dtime=dtime
                               )
    else:
        with conf._lock(condition, notify=False):
            isDaemon = True
            while isDaemon:
                transferLogs(ctx, conf, condition, dtime, logTypes, None, npt)

                # Run only once if not in the daemon mode
                isDaemon = conf._isDaemon

        res = conf.status

    app.logger.info('Polling: stop polling log events.. pid=%s, tid=%s' % (pid, tid))
    app.logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    return res


class bitglassapi:

    Initialize = Initialize

    # TODO Implement OverrideConfig(), it will also validate all settings (the validation is to be moved from UI)

    # Low level (overriding settings params)
    restCall = restCall

    RestParamsLogs = RestParamsLogs
    RestParamsConfig = RestParamsConfig

    RestCall = RestCall

    # Higher level calls relying on serialized data and synchronization
    PollLogs = PollLogs

    def __init__(self, ctx=None):
        if ctx is None:
            # Use default callbacks
            ctx = self

        self.ctx = ctx

    # Default callbacks command mode without explicit context (like Splunk)
    def bgPushLogEvent(self, d, address, logTime):
        # Additional processing for the script
        from app import cli
        cli.pushLog(d, address, logTime)

    def bgFlushLogEvents(self):
        from app import cli
        cli.flushLogs()

    def bgLoadConfig(self, conf):
        from app import cli
        cli.loadConfiguration(conf)


def startWorkerThread(conf, isDaemon=True, bgapi=None):

    Initialize(bgapi, _logger=app.logger, _conf=conf)

    condition = Condition()
    thread = Thread(target=PollLogs, args=(bgapi, conf, None, condition))

    conf._isDaemon = isDaemon
    thread.start()
    if not isDaemon:
        thread.join()
    return condition


if __name__ == '__main__':

    Initialize(None)

    # Only for debugging full context cli variants so that can use one debug setting for all
    if conf._isEnabled('debug'):
        main(app.logger, conf, bitglassapi)

    # Start the worker thread explicitly if main() above didn't exit()
    startWorkerThread(conf, False, bitglassapi())
