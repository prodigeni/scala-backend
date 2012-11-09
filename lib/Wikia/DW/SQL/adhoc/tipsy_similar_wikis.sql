SELECT w.url,
       sub.*
  FROM (SELECT pv.wiki_id,
               COUNT(1) AS users,
               SUM(pv.pageviews) AS pageviews
          FROM (SELECT DISTINCT user_id
                  FROM dimension_wikis w
                  JOIN rollup_wiki_user_pageviews r
                    ON r.period_id = 3
                   AND r.time_id = '2011-11-01'
                   AND r.wiki_id = w.wiki_id
                 WHERE w.url = 'http://rage.wikia.com/') u
          JOIN rollup_wiki_user_pageviews pv
            ON pv.period_id = 3
           AND pv.time_id = '2011-11-01'
           AND pv.user_id = u.user_id
         GROUP BY pv.wiki_id
         ORDER BY COUNT(1) DESC
         LIMIT 40) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 ORDER BY sub.users DESC

