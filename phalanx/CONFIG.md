# Phalanx properties help
## Logging configuration
* org.slf4j.simpleLogger.defaultLogLevel=info
Default logging level
* org.slf4j.simpleLogger.log.Main=
* org.slf4j.simpleLogger.log.MainService=
* org.slf4j.simpleLogger.log.NewRelic=
* org.slf4j.simpleLogger.log.RuleSystem=
* org.slf4j.simpleLogger.log.RuleSystemLoader=
* org.slf4j.simpleLogger.log.Scribe=

## Main configuration
* com.wikia.phalanx.detailedStats=true
Keep detailed statistics
* com.wikia.phalanx.notifyNodes=
Space separated list of other nodes to notify
* com.wikia.phalanx.port=4666
HTTP listening port
* com.wikia.phalanx.serviceThreadCount=0
Number of main service threads, or 0 for auto value
* com.wikia.phalanx.userCacheMaxSize=131071
Size of LRU cache for user matching
* com.wikia.phalanx.workerGroups=0
Split each matching work into n parallel groups, or 0 for auto value

## Scribe configuration
* com.wikia.phalanx.scribe=discard
Scribe type: send, buffer or discard
* com.wikia.phalanx.scribe.flushperiod=1000
Scribe buffer flush period (milliseconds)
* com.wikia.phalanx.scribe.host=localhost
Scribe host name
* com.wikia.phalanx.scribe.port=1463
Scribe TCP port

