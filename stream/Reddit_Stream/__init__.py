import json
import time
import datetime
import os
import traceback
from threading import Thread, RLock
import urllib2
import urllib

jsonPage={}
class MessageDispatcher(Thread):
    """Separate thread for distributing the comments so that the reader
    thread doesn't lose comments by waiting for the listeners.
    """
    def __init__(self):
        Thread.__init__(self)
        self.continueLoop = True
        self.messageLock = RLock()
        self.listenerLock = RLock()
        self.messages = []
        self.listeners = []

    def push(self, message):
        with self.messageLock:
            self.messages.append(message)

    def addListener(self, listener):
        with self.listenerLock:
            self.listeners.append(listener)

    def removeListener(self, type, listener):
        with self.listenerLock:
            self.listeners.remove(listener)

    def run(self):
        while self.continueLoop:
            time.sleep(0.05)
            with self.messageLock:
                with self.listenerLock:
                    for message in self.messages:
                        for listener in self.listeners:
                            listener.push(message)
                    self.messages = []

    def stop(self):
        self.continueLoop = False

class CommentStream(Thread):
    def __init__(self, userAgent, waitSeconds=10, maxLag=0, subreddit=None):
        """Set maxLag > 0 if you want to skip ahead when you fall more than
        maxLag seconds behind. Don't use this on subreddits where comments
        are very rare.
        """
        Thread.__init__(self)

        self.headers = {'User-Agent': userAgent}
        self.waitSeconds = waitSeconds
        self.maxLag = maxLag

        if subreddit is None:
            self.subredditUrl = 'http://reddit.com/comments.json'
        else:
            self.subredditUrl = 'http://reddit.com/r/%s/comments.json' % subreddit

        self.messageDispatcher = MessageDispatcher()
        self.continueLoop = True
        self.placeHolder = None
        self.before = None
        self.lastCode = 0

    def run(self):
        self.messageDispatcher.start()

        print 'Starting comment stream...'
        self.loop()
        print 'Shutting comment stream...'

        self.messageDispatcher.stop()

    def loop(self):
        while True:
            lastReqTime = time.time()

            self.checkComments()

            while time.time() - lastReqTime < self.waitSeconds:
                time.sleep(0.1)
                if not self.continueLoop:
                    return

    def stop(self):
        self.continueLoop = False

    def addListener(self, func):
        self.messageDispatcher.addListener(func)

    def removeListener(self, func):
        self.messageDispatcher.removeListener(func)

    def checkComments(self):
        data = {
            'limit': 100 # This is the maximum.
        }
        if self.before != None:
            data['before'] = self.before
        url = self.subredditUrl + '?' + urllib.urlencode(data)
            
        try:
            req = urllib2.Request(url, headers=self.headers)
            page = urllib2.urlopen(req).read()
            jsonPage = json.loads(page)
        except:
            self.before = None
            self.messageDispatcher.push({
                'type': 'error',
                'errorType': 'http',
                'trace': traceback.format_exc()
            })

        if not jsonPage.has_key('data'):
            self.before = None
            self.messageDispatcher.push({
                'type': 'error',
                'errorType': 'noData',
            })
            return
            
        jsonData = jsonPage['data']
        comments = []
        for child in jsonData['children']:
            comment = child['data']
            comment['_code'] = int(comment['id'], 36)
            comments.append(comment)

        if len(comments) == 0:
            self.before = None
            return

        comments.sort(key=lambda k: k['_code'])
        self.before = 't1_' + comments[-1]['id']

        newComments = []
        for comment in comments:
            if comment['_code'] > self.lastCode:
                newComments.append(comment)

        if len(newComments) == 0:
            return

        self.lastCode = newComments[-1]['_code']

        self.messageDispatcher.push({
            'type': 'comments',
            'comments': newComments
        })

        utcNow = time.mktime(datetime.datetime.utcnow().timetuple())
        lag = utcNow - newComments[-1]['created_utc']
        if self.maxLag > 0 and lag > self.maxLag:
            self.before = None
            self.messageDispatcher.push({
                'type': 'skipping',
            })

class LogSequence:
    def __init__(self, logDir, maxMinor=1000):
        self.logDir = logDir
        self.maxMinor = maxMinor
        self.logMajor = 0
        self.logMinor = 0
        self.dirFormat = '%s/%08d'
        self.fileFormat = self.dirFormat + '/%010d.json'

        # Creating log dir
        if not os.path.isdir(self.logDir):
            os.makedirs(self.logDir, 0755)
            os.mkdir(self.dirFormat % (self.logDir, self.logMajor))

        majorFiles = os.listdir(self.logDir)
        major = max(majorFiles)
        self.logMajor = int(major)

        minorFiles = os.listdir(os.path.join(self.logDir, major))
        if len(minorFiles) == 0:
            return
        self.logMinor = int(max(minorFiles).split('.')[0])

        self.shiftLog()

    def shiftLog(self):
        self.logMinor += 1
        if self.logMinor >= self.maxMinor:
            self.logMajor += 1
            self.logMinor = 0
            os.mkdir(self.dirFormat % (self.logDir, self.logMajor))

    def getNewLogFile(self):
        logFile = self.fileFormat % \
                (self.logDir, self.logMajor, self.logMinor)

        self.shiftLog() 

        return logFile

    def getLogFiles(self):
        for d in sorted(os.listdir(self.logDir)):
            dd = self.logDir + '/' + d
            for f in sorted(os.listdir(dd)):
                yield dd + '/' + f

class StreamListener(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.pauseTime = 0.020
        self.continueLoop = True
        self.messagesLock = RLock()
        self.messages = []

    def push(self, message):
        with self.messagesLock:
            self.messages.append(message)

    def run(self):
        while self.continueLoop:
            time.sleep(self.pauseTime)
            with self.messagesLock:
                if len(self.messages) == 0:
                    continue
                message = self.messages.pop(0)
            self.processMessage(message)

    def stop(self):
        self.continueLoop = False

    def processMessage(self, message):
        raise NotImplementedError("You forgot something...")

class LogWriterListener(StreamListener):
    def __init__(self, logDir):
        StreamListener.__init__(self)
        self.logSequence = LogSequence(logDir)

    def processMessage(self, message):
        logFile = self.logSequence.getNewLogFile()
        out = open(logFile, 'w')
        out.write(json.dumps(message))
        out.close()