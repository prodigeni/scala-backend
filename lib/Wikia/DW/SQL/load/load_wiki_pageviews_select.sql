SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       wiki_id,
       COUNT(1) AS pageviews
  FROM statsdb.fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id
