REPLACE INTO statsdb.rollup_wiki_visit_metrics (
    period_id,
    time_id,
    wiki_id,
    visits,
    visitors,
    pageviews_per_visit
)
SELECT @new_period_id           := [period_id] AS period_id,
       @new_time_id             := [time_id] AS time_id,
       @new_wiki_id             := v.wiki_id,
       @new_visits              := COUNT(1) AS visits,
       @new_visitors            := COUNT(DISTINCT v.visitor_id) AS visitors,
       @new_pageviews_per_visit := AVG(v.pageviews) AS pageviews_per_visit
  FROM statsdb_etl.etl_period_times pt
  JOIN rollup_wiki_visits v
    ON v.period_id = [period_id]
   AND v.time_id = pt.time_id
 WHERE pt.period_id = [period_id]
   AND pt.time_id BETWEEN (SELECT MAX(time_id) FROM rollup_wiki_visit_metrics WHERE period_id = [period_id])
                      AND now()
 GROUP BY time_id,
          wiki_id
