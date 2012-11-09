SELECT wiki_id,
       user_id,
       log_id,
       event_type,
       event_date
  FROM chatlog
 WHERE log_id > [last_id]
   AND event_date > TIMESTAMP('2011-01-01')

