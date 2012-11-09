SELECT w.wiki_id,
       w.url,
       pv1.pageviews AS week1,
       pv2.pageviews AS week2,
       CAST(pv2.pageviews AS SIGNED) - CAST(pv1.pageviews AS SIGNED) AS diff,
       ROUND(100 * (CAST(pv2.pageviews AS SIGNED) - CAST(pv1.pageviews AS SIGNED)) / pv1.pageviews, 2) AS diff_percent
  FROM (SELECT *
          FROM rollup_wiki_pageviews
         WHERE period_id = 2
           AND time_id = '2011-11-27'
         ORDER BY pageviews DESC
         LIMIT 500) pv1
  JOIN dimension_wikis w
    ON w.wiki_id = pv1.wiki_id
  LEFT JOIN rollup_wiki_pageviews pv2
    ON pv2.period_id = 2
   AND pv2.time_id = '2011-12-11'
   AND pv2.wiki_id = pv1.wiki_id
 ORDER BY pv1.pageviews DESC


