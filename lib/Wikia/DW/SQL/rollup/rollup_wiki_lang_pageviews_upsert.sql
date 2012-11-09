INSERT INTO statsdb.rollup_wiki_lang_pageviews (
    period_id,
    time_id,
    lang,
    pageviews
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       w.lang,
       @new_pageviews := COUNT(1)
  FROM statsdb.fact_pageview_events e
  JOIN dimension_wikis w
    ON w.wiki_id = e.wiki_id
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          w.lang
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

