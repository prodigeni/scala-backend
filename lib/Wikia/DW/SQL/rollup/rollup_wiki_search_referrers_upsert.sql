INSERT INTO statsdb.rollup_wiki_search_referrers (
    period_id,
    time_id,
    wiki_id,
    search_domain_id,
    search_term_id,
    pageviews
)
SELECT @new_period_id := [period_id] AS period_id,
       @new_time_id   := [time_id]   AS time_id,
       @new_wiki_id   := wiki_id,
       @new_search_domain_id := r.search_domain_id,
       @new_search_term_id   := r.search_term_id,
       @new_pageviews := COUNT(1) AS pageviews
  FROM fact_referrer_events r
  JOIN fact_pageview_events pv
    ON pv.source   = r.source
   AND pv.file_id  = r.file_id
   AND pv.event_id = r.event_id
   AND pv.event_ts = r.event_ts
 WHERE r.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND r.file_id = [file_id]
   AND r.search_domain_id != 0
 GROUP BY pv.wiki_id,
          r.search_domain_id,
          r.search_term_id
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews

