#!/bin/bash

# Set a path to the specially altered garb gem.  This has an http_proxy setting added to it
BACKEND_PATH=/usr/wikia/backend
GARB_PATH=$BACKEND_PATH/lib/gems/garb-0.7.6/lib

rvm use ree >> /dev/null 2>&1
ruby -I $GARB_PATH $BACKEND_PATH/bin/ga_reports/unified_loader.rb >> /tmp/ga_loader.log 2>&1
