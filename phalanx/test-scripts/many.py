#!/usr/bin/python
import sys, os
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

numberOfRequests = int(os.environ.get('REQUESTS', 3000))
concurrentRequests = int(os.environ.get('CONCURRENT', 10))
dotProgress = int(os.environ.get('PROGRESS', 100))
baseUrl = os.environ.get('URL', "http://localhost:4666")

class Dots:
	def __init__(self):
		self.times = []
	
	def add(self, t):
		self.times.append(t)
		if len(self.times) % dotProgress == 0:
			sys.stdout.write(".")
			sys.stdout.flush()	

dots = Dots()

def makeRequest(x):
	startTime = time()
	d = getPage("%s/match?type=user&content=%s" % (baseUrl, x))
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
		reactor.stop()

if __name__ == '__main__':
	if os.environ.get('NORANDOM'):
		allIps = ["100.100.100.100" for j in range(0, numberOfRequests) ] # `numberOfRequests` same IPs
		print "Same requests"
	else:
		allIps = ["%d.%d.%d.%d" % tuple([randint(1,255) for i in range(0,4)]) for j in range(0, numberOfRequests) ] # `numberOfRequests` random IPs
	startTime = time()
	reactor.callWhenRunning(oneBatch, 0, allIps)
	reactor.run()
	endTime = time()
	diff = endTime-startTime
	print "Finished in %0.02f seconds, %d req/s" % (diff, numberOfRequests/diff)
	if len(dots.times):
		print "Average request time: %f s" % mean(dots.times)
		print "Maximum request time: %f s" % max(dots.times)
		
