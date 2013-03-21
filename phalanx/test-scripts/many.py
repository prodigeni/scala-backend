#!/usr/bin/python
import sys, os, urllib
from time import time
from random import randint
from twisted.web.client import getPage
from twisted.internet import reactor
from twisted.internet.defer import DeferredList
from twisted.python import log
try:
	from scipy import mean
except ImportError:
	def mean(l):
		return sum(l)/float(len(l)) # not the numerically good to way to do it, but let's try anyway...

numberOfRequests = int(os.environ.get('REQUESTS', 5000))
concurrentRequests = int(os.environ.get('CONCURRENT', 10))
dotProgress = int(os.environ.get('PROGRESS', 100))
baseUrl = os.environ.get('URL', "http://localhost:4666")
longOnes = os.environ.get('LONGONES', 50)

class Dots:
	def __init__(self):
		self.times = []
	
	def add(self, t):
		self.times.append(t)
		if len(self.times) % dotProgress == 0:
			sys.stdout.write(".")
			sys.stdout.flush()	

dots = Dots()
HEADERS = { 'referer': "test-scripts/many.py",
					  'Content-Type':'application/x-www-form-urlencoded'}

def makeRequest(x):
	startTime = time()	
	typ = "content" if len(x)>16 else "user"
	d = getPage("%s/match?type=%s" % (baseUrl,typ), method = b'POST', headers = HEADERS,
							postdata = urllib.urlencode(dict(content=x)))
	@d.addCallback
	def cb(result):
		dots.add(time() - startTime)
		return result
	return d

def oneBatch(prev, ipList):
	toDo, left = ipList[:concurrentRequests], ipList[concurrentRequests:]
	if toDo:
		d = DeferredList([makeRequest(x) for x in toDo])
		@d.addCallback
		def c(results):
			return prev + len([x for x in results if x[1] in ("[]", "ok\n")])
		d.addErrback(log.err)
		d.addBoth(oneBatch, left)
	else:
		print "\n%d requests ended in empty match" % prev
		d = getPage("%s/stats/avg" % (baseUrl, ))
		@d.addCallback
		def c2(time):
			print "Average matching time in server: %s" % time
			reactor.stop()

if __name__ == '__main__':
	if os.environ.get('NORANDOM'):
		allIps = ["100.100.100.100" for j in range(0, numberOfRequests) ] # `numberOfRequests` same IPs
		print "Same requests"
	else:
		allIps = [("%d.%d.%d.%d" % tuple([randint(1,255) for i in range(0,4)]) if (longOnes == 0 or j % longOnes != 0) else
			        "".join(chr(randint(32, 122)) for x in range(1, 1000)))
							for j in range(0, numberOfRequests) ] # `numberOfRequests` random IPs
	startTime = time()
	reactor.callWhenRunning(oneBatch, 0, allIps)
	reactor.run()
	endTime = time()
	diff = endTime-startTime
	print "Finished in %0.02f seconds, %d req/s" % (diff, numberOfRequests/diff)
	if len(dots.times):
		print "Average request time: %f s" % mean(dots.times)
		print "Maximum request time: %f s" % max(dots.times)
		
