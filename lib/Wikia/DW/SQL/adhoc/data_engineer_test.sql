SELECT CONCAT(event_ts, ' [INFO] /', CASE WHEN wiki_id % 13 IN (1,3,5,7) AND user_id % 7 = 2 THEN 'edit' WHEN wiki_id % 13 IN (2,4,6,8) AND user_id % 7 = 4 THEN 'delete' ELSE 'pageview' END, '?wiki_id=', wiki_id, '&user_id=', user_id) AS event
  FROM (SELECT event_ts,
               wiki_id+floor(rand()*13)+13 AS wiki_id,
               user_id+floor(rand()*13)+13 AS user_id
          FROM fact_pageview_events
         WHERE event_ts >= '2012-01-01 00:00:00'
           AND event_ts <  '2012-01-01 05:00:00'
       ) s
 WHERE user_id >= 0
   AND wiki_id >= 0

