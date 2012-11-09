SELECT DATE(pt.time_id) AS "Week",
       AVG(r.pageviews) AS "PV / Visit"
  FROM statsdb_etl.etl_period_times pt
  JOIN rollup_wiki_visits r
    ON r.period_id = 2 
   AND r.time_id = pt.time_id
 WHERE pt.period_id = 2 
   AND pt.time_id BETWEEN DATE_SUB(now(), INTERVAL 30 DAY)
                      AND now()
 GROUP BY DATE(pt.time_id)
 ORDER BY pt.time_id DESC
