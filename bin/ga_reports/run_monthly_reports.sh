#!/bin/bash

cd /usr/wikia/backend/bin/ga_reports

/usr/local/rvm/bin/rvm use ree >> /dev/null 2>&1
ruby /usr/wikia/backend/bin/ga_reports/monthly_report.rb >> /tmp/monthly_report.log 2>&1
