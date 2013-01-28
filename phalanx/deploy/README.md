# Deploying Phalanx #

## Dependencies in default configuration
* Java 7
* Runit
* Scribe

## Phalanx in production requires that following files should be installed:

### in /usr/wikia/phalanx/lib:
* newrelic.jar
* newrelic.yml
* phalanx-server.jar
* phalanx-test

### in /etc/sv/phalanx
* runit/run

### in /etc/sv/phalanx/log
* runit/log/run

### in /usr/conf/wikia/current
* DB.yml - standard wikifactory configuration used by Wikia
* phalanx.default.properties - copied from phalanx.default.properties
* phalanx.properties - if you wish replace default configuration


