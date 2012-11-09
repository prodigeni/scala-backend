SELECT w.*, pv.pageviews
  FROM rollup_wiki_pageviews pv
  LEFT JOIN dimension_wikis w
    ON w.wiki_id = pv.wiki_id
 WHERE pv.period_id = 1
   AND pv.time_id = '2011-12-01'
 ORDER BY pv.pageviews DESC
 LIMIT 10
