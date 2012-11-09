INSERT INTO statsdb.rollup_wiki_provider_views (
    period_id,
    time_id,
    wiki_id,
    provider,
    views
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       wiki_id,
       provider,
       @new_views := COUNT(1) AS views
  FROM statsdb.fact_lightbox_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.ga_category = 'lightbox'
   AND e.ga_action   = 'view'
   AND e.ga_label   IN ('video', 'video-inline')
   AND e.title IS NOT NULL
   AND e.provider IS NOT NULL
 GROUP BY period_id,
          time_id,
          wiki_id,
          provider
    ON DUPLICATE KEY UPDATE views = views + @new_views

