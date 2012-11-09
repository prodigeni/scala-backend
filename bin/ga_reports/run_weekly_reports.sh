#!/bin/bash

DATE=`date +"%m-%d-%Y"`

cd /usr/wikia/backend/bin/ga_reports

/usr/local/rvm/bin/rvm use ree >> /dev/null 2>&1
ruby /usr/wikia/backend/bin/ga_reports/weekly_movers.rb >> /tmp/weekly_movers-$DATE.log
ruby /usr/wikia/backend/bin/ga_reports/weekly_report_entertainment.rb >> /tmp/weekly_entertainment-$DATE.log
ruby /usr/wikia/backend/bin/ga_reports/weekly_report.rb >> /tmp/weekly_report-$DATE.log
ruby /usr/wikia/backend/bin/ga_reports/weekly_report_adam.rb >> /tmp/weekly_adam-$DATE.log
ruby /usr/wikia/backend/bin/ga_reports/weekly_report_gaming.rb >> /tmp/weekly_gaming-$DATE.log
