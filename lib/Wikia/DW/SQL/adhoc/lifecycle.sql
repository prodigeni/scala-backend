SELECT w.wiki_id,
       w.domain,
       w.hub_name,
       w.created_at,
       DATE_SUB(DATE(DATE_FORMAT(w.created_at, '%Y-%m-01')), INTERVAL (MONTH(w.created_at) + 2) % 3 MONTH) AS creation_quarter,
       ROUND(DATEDIFF(now(), w.created_at),2) AS age_in_days,
       pageviews_56day,
       pageviews_28day,
       pageviews_14day,
       pageviews_7day,
       edits_56day,
       edits_28day,
       edits_14day,
       edits_7day,
       editors_56day,
       editors_28day,
       editors_14day,
       editors_7day
  FROM dimension_wikis w
  JOIN (
        SELECT r.wiki_id,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 56 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL 29 DAY)
                        THEN r.pageviews ELSE 0 END) AS pageviews_56day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
                        THEN r.pageviews ELSE 0 END) AS pageviews_28day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 14 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL  8 DAY)
                        THEN r.pageviews ELSE 0 END) AS pageviews_14day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 7 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL 1 DAY)
                        THEN r.pageviews ELSE 0 END) AS pageviews_7day
          FROM rollup_wiki_pageviews r
         WHERE r.period_id = 1
           AND r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 56 DAY)
                             AND DATE_SUB(DATE(now()), INTERVAL 1 DAY)
         GROUP BY r.wiki_id
       ) pv
    ON pv.wiki_id = w.wiki_id
  JOIN (
        SELECT r.wiki_id,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 56 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL 29 DAY)
                        THEN r.creates + r.edits ELSE 0 END) AS edits_56day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
                        THEN r.creates + r.edits ELSE 0 END) AS edits_28day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 14 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL  8 DAY)
                        THEN r.creates + r.edits ELSE 0 END) AS edits_14day,
               SUM(CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 7 DAY)
                                           AND DATE_SUB(DATE(now()), INTERVAL 1 DAY)
                        THEN r.creates + r.edits ELSE 0 END) AS edits_7day,
               COUNT(DISTINCT CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 56 DAY)
                                                      AND DATE_SUB(DATE(now()), INTERVAL 29 DAY)
                                   THEN r.user_id ELSE null END) AS editors_56day,
               COUNT(DISTINCT CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 28 DAY)
                                                      AND DATE_SUB(DATE(now()), INTERVAL  1 DAY)
                                   THEN r.user_id ELSE null END) AS editors_28day,
               COUNT(DISTINCT CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 14 DAY)
                                                      AND DATE_SUB(DATE(now()), INTERVAL  8 DAY)
                                   THEN r.user_id ELSE null END) AS editors_14day,
               COUNT(DISTINCT CASE WHEN r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 7 DAY)
                                                      AND DATE_SUB(DATE(now()), INTERVAL 1 DAY)
                                   THEN r.user_id ELSE null END) AS editors_7day
          FROM rollup_wiki_user_events r
         WHERE r.period_id = 1
           AND r.time_id BETWEEN DATE_SUB(DATE(now()), INTERVAL 56 DAY)
                             AND DATE_SUB(DATE(now()), INTERVAL 1 DAY)
           AND r.creates > 0
           AND r.edits > 0
         GROUP BY r.wiki_id
       ) e
    ON e.wiki_id = w.wiki_id

