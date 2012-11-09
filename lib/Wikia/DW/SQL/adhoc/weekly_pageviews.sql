SELECT DATE(pt.time_id) AS time_id,
       SUM(r.pageviews) AS pageviews
  FROM statsdb_etl.etl_period_times pt
  JOIN rollup_wiki_pageviews r
    ON r.period_id = 2
   AND r.time_id = pt.time_id
 WHERE pt.period_id = 2
   AND pt.time_id BETWEEN '2011-12-01'
                      AND now()
 GROUP BY pt.time_id
 ORDER BY pt.time_id DESC
