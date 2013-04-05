# Phalanx properties help
## Logging configuration (using slf4j simpleLogger)
* org.slf4j.simpleLogger.defaultLogLevel=info
Default logging level
* org.slf4j.simpleLogger.log.Main=
* org.slf4j.simpleLogger.log.MainService=
* org.slf4j.simpleLogger.log.NewRelic=
* org.slf4j.simpleLogger.log.RuleSystem=
* org.slf4j.simpleLogger.log.RuleSystemLoader=
* org.slf4j.simpleLogger.log.Scribe=

## Network configuration
* com.wikia.phalanx.backlog=1000
Listening socket backlog size
* com.wikia.phalanx.cancelOnHangup=true
Cancel requests if connection lost
* com.wikia.phalanx.keepAlive=true
Use HTTP 1.1 keep alives
* com.wikia.phalanx.maxConcurrentRequests=100
Netty: maximum requests server at the same time
* com.wikia.phalanx.maxIdleTime=3
How long to wait before closing a keep alive connection, in seconds
* com.wikia.phalanx.notifyNodes=
Space separated list of other nodes to notify
* com.wikia.phalanx.port=4666
HTTP listening port
* com.wikia.phalanx.recvBufferSize=524288
Netty reveive buffer size
* com.wikia.phalanx.sendBufferSize=131072
Netty send buffer size

## Performance tuning
* com.wikia.phalanx.detailedStats=true
Keep detailed statistics
* com.wikia.phalanx.serviceThreadCount=0
Number of main service threads, or 0 for auto value
* com.wikia.phalanx.userCacheMaxSize=131071
Size of LRU cache for user matching
* com.wikia.phalanx.workerGroups=0
Split each matching work into n parallel groups, or 0 for auto value

## Scribe configuration - used to relay information about successful matches
* com.wikia.phalanx.scribe=discard
Scribe type: send, buffer or discard
* com.wikia.phalanx.scribe.flushperiod=1000
Scribe buffer flush period (milliseconds)
* com.wikia.phalanx.scribe.host=localhost
Scribe host name
* com.wikia.phalanx.scribe.port=1463
Scribe TCP port

