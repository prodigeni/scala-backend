SELECT w.wiki_id,
       w.dbname,
       w.sitename,
       w.url,
       w.domain,
       w.founding_user_id,
       w.public,
       w.lang,
       w.lang_id,
       w.hub_id,
       w.hub_name,
       w.created_at,
       sub.rank
  FROM (
        SELECT sub1.wiki_id,
               sub1.pageviews,
               COUNT(sub2.wiki_id) AS rank
          FROM (
                SELECT r.wiki_id,
                       SUM(r.pageviews) AS pageviews
                  FROM statsdb_mart.rollup_wiki_pageviews r
                 WHERE r.period_id = 3 
                   AND r.time_id > DATE_SUB(now(), INTERVAL 3 MONTH)
                   AND r.wiki_id != 0
                 GROUP BY r.wiki_id
                 ORDER BY SUM(r.pageviews) DESC, r.wiki_id
                 LIMIT 50000
               ) sub1
          JOIN (
                SELECT r.wiki_id,
                       SUM(r.pageviews) AS pageviews
                  FROM statsdb_mart.rollup_wiki_pageviews r
                 WHERE r.period_id = 3 
                   AND r.time_id > DATE_SUB(now(), INTERVAL 3 MONTH)
                   AND r.wiki_id != 0
                 GROUP BY r.wiki_id
                 ORDER BY SUM(r.pageviews) DESC, r.wiki_id
                 LIMIT 50000
               ) sub2
            ON sub1.pageviews < sub2.pageviews
            OR (sub1.pageviews = sub2.pageviews AND sub1.wiki_id >= sub2.wiki_id)
         GROUP BY sub1.wiki_id
         ORDER BY COUNT(1)
       ) sub 
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id

