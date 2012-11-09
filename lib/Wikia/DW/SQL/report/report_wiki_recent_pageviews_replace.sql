REPLACE INTO statsdb_mart.report_wiki_recent_pageviews (
    wiki_id,
    hub_name,
    lang,
    pageviews_7day,
    pageviews_30day,
    pageviews_90day
)
SELECT w.wiki_id,
       w.hub_name,
       w.lang,
       sub.pageviews_7day,
       sub.pageviews_30day,
       sub.pageviews_90day
  FROM (
        SELECT wiki_id,
               SUM(CASE WHEN time_id >= CURDATE() - INTERVAL  7 DAY THEN pageviews ELSE 0 END) AS pageviews_7day,
               SUM(CASE WHEN time_id >= CURDATE() - INTERVAL 30 DAY THEN pageviews ELSE 0 END) AS pageviews_30day,
               SUM(CASE WHEN time_id >= CURDATE() - INTERVAL 90 DAY THEN pageviews ELSE 0 END) AS pageviews_90day
          FROM rollup_wiki_pageviews
         WHERE period_id = 1
           AND time_id >= CURDATE() - INTERVAL 90 DAY 
         GROUP BY wiki_id
       ) sub 
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id

