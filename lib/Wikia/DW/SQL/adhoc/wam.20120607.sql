SELECT sub.wiki_id,
       sub.pageviews_28day,
       sub.pageviews_7day,
       SUM(creates + edits) AS edits_28day,
       SUM(CASE WHEN e.time_id >= DATE_SUB(DATE(now()), INTERVAL 7 DAY) THEN creates + edits ELSE 0 END) AS edits_7day,
       COUNT(DISTINCT CASE WHEN creates + edits > 0 THEN e.user_id ELSE null END) AS editors_28day,
       COUNT(DISTINCT CASE WHEN creates + edits > 0 AND e.time_id >= DATE_SUB(DATE(now()), INTERVAL 7 DAY) THEN e.user_id ELSE null END) AS editors_7day
  FROM (
        SELECT w.wiki_id,
               SUM(pv.pageviews) AS pageviews_28day,
               SUM(CASE WHEN pv.time_id >= DATE_SUB(DATE(now()), INTERVAL 7 DAY) THEN pv.pageviews ELSE 0 END) AS pageviews_7day
          FROM dimension_top_wikis w
          LEFT JOIN statsdb_mart.rollup_wiki_pageviews pv
            ON pv.wiki_id = w.wiki_id
           AND pv.period_id = 1
           AND pv.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                              AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
         WHERE w.rank <= 10
         GROUP BY w.wiki_id
       ) sub
  LEFT JOIN statsdb_mart.rollup_edit_events e
    ON e.wiki_id = sub.wiki_id
   AND e.period_id = 1
   AND e.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                     AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
 GROUP BY sub.wiki_id
 ORDER BY 2 DESC




