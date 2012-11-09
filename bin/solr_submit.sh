#!/bin/bash

cd /var/spool/solr

process_events() {
  events_count=`ls *event*.xml 2>/dev/null |wc -l`
  if [ $events_count -gt 0 ] ; then
    read_loop
  fi
}

read_loop() {
for i in `ls *event*.xml 2>/dev/null`; do
  if [ -e "$i" ] ; then
  submit_event "$i"
  if [ $? == 0 ]; then 
    rm $i
  else 
    mv $i failed
    gzip -f failed/$i 
  fi
  fi
done
}

process_bulk() {

  i=`ls *bulk*xml 2>/dev/nul |head -1`
  if [ -e "$i" ]
  then
  submit_event "$i"
  if [ $? == 0 ]; then 
    rm $i
  else 
    mv $i failed
    gzip -f failed/$i 
  fi
  fi
   
}

submit_event() {
  java -jar -Durl="http://search-s6:8983/solr/update" /opt/apache-solr/example/exampledocs/post.jar "$1" 
  return $? 
}

while true
do 
  process_events;
  process_bulk;
done

