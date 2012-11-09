SELECT w.wiki_id,
       w.url,
       pv.time_id,
       pv.pageviews
  FROM rollup_wiki_pageviews pv
  LEFT JOIN dimension_wikis w
    ON w.wiki_id = pv.wiki_id
 WHERE pv.period_id = 1
   AND pv.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                      AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
   AND pv.wiki_id = 1706
 ORDER BY pv.time_id DESC

