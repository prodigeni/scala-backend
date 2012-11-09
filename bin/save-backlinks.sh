#!/bin/bash

cd /var/spool/solr

while true
 do
  for i in `ls | grep .sql$`
  do
    echo -e `date -d @\`date +%s\`` "\tSubmitting $i"
    mysql -uroot -proot -h search-s1 backlinks < $i
  if [ $? == 0 ]; then
      echo -e `date -d @\`date +%s\`` "\t$i Submitted Successfully"
     mv $i processed
     gzip -f processed/$i
  else
     mv $i failed
     gzip -f failed/$i
  fi
  sleep 5
 done
done
