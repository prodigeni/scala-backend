INSERT INTO statsdb.rollup_hub_beacon_pageviews (
    period_id,
    time_id,
    hub_id,
    beacon,
    pageviews
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       CASE e.article_id WHEN 3867 THEN 2
                         WHEN 3876 THEN 3
                         WHEN 3875 THEN 9
                         WHEN 1461 THEN 4
                                   ELSE 0 END AS hub_id,
       e.beacon,
       @new_pageviews := COUNT(1)
  FROM statsdb.fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.wiki_id = 80433
   AND e.article_id IN (3867,3876,3875,1461)
 GROUP BY period_id,
          time_id,
          hub_id,
          beacon
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

