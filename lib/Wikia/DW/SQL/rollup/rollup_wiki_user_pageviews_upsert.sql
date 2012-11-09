INSERT INTO statsdb.rollup_wiki_user_pageviews (
    period_id,
    time_id,
    wiki_id,
    user_id,
    pageviews
)
SELECT @new_period_id := [period_id] AS period_id,
       @new_time_id   := [time_id]   AS time_id,
       @new_wiki_id   := wiki_id,
       @new_user_id   := user_id,
       @new_pageviews := COUNT(1) AS pageviews
  FROM statsdb.fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id,
          user_id
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

