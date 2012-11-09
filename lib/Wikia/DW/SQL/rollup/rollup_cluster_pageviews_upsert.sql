INSERT INTO rollup_cluster_pageviews (
    period_id,
    time_id,
    cluster,
    pageviews
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       w.cluster,
       @new_pageviews := COUNT(1) AS new_pageviews
  FROM fact_pageview_events e
  JOIN dimension_wikis w
    ON w.wiki_id = e.wiki_id
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          w.cluster
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

