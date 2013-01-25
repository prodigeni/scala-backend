#!/usr/bin/python
import sys
from time import time
from random import randint
from twisted.web.client import getPage
from twisted.internet import reactor
from twisted.internet.defer import DeferredList
from twisted.python import log

numberOfRequests = 100
concurrentRequests = 10
baseUrl = "http://localhost:8080"

def oneBatch(prev, ipList):
	toDo, left = ipList[:concurrentRequests], ipList[concurrentRequests:]
	if toDo:
		d = DeferredList([getPage("%s/match?type=user&content=%s" % (baseUrl, x)) for x in toDo])
		@d.addCallback
		def c(results):
			sys.stdout.write(".")
			return prev + len([x for x in results if x[1] in ("[]", "ok\n")])
		d.addErrback(log.err)
		d.addBoth(oneBatch, left)
	else:
		print "\n%d requests ended in empty match" % prev
		reactor.stop()

if __name__ == '__main__':
	allIps = ["%d.%d.%d.%d" % tuple([randint(1,255) for i in range(0,4)]) for j in range(0, numberOfRequests) ] # `numberOfRequests` random IPs
	startTime = time()
	reactor.callWhenRunning(oneBatch, 0, allIps)
	reactor.run()
	endTime = time()
	diff = endTime-startTime
	print "Finished in %0.02f seconds, %d req/s" % (diff, numberOfRequests/diff)
