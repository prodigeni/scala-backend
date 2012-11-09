SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       wiki_id,
       title,
       COUNT(1) AS views
  FROM statsdb.fact_lightbox_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.ga_category = 'lightbox'
   AND e.ga_action   = 'view'
   AND e.ga_label   IN ('video', 'video-inline')
   AND e.title IS NOT NULL
 GROUP BY period_id,
          time_id,
          wiki_id,
          title
