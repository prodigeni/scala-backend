INSERT INTO statsdb.rollup_wiki_beacon_pageviews (
    period_id,
    time_id,
    wiki_id,
    beacon,
    pageviews
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       e.wiki_id,
       e.beacon,
       @new_pageviews := COUNT(1)
  FROM statsdb.fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id,
          beacon
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

