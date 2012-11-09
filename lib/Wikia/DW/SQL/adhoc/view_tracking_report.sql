SELECT sub.wiki_id AS "Wiki",
       MAX(sub.domain) AS "Domain",
       SUM(sub.pageviews) AS "Pageviews",
       SUM(sub.views)     AS "Total Video Views",
       CASE WHEN SUM(sub.pageviews) = 0 THEN 0 ELSE ROUND(SUM(sub.views) / SUM(sub.pageviews), 2) END AS "Video Views / Pageviews",
       SUM(sub.premium_views) AS "Premium Video Views",
       SUM(sub.views) - SUM(sub.premium_views) AS "Non-Premium Video Views",
       FLOOR(0.64 * SUM(sub.premium_views))    AS "Pre-Roll Inventory"
  FROM (
        SELECT pv.wiki_id,
               MAX(pv.domain) AS domain,
               pv.time_id,
               IFNULL(MAX(pv.pageviews),0) AS pageviews,
               IFNULL(SUM(vv.views),0)     AS views,
               IFNULL(SUM(CASE WHEN vv.provider IN ('ign','screenplay') THEN vv.views ELSE 0 END),0) AS premium_views
          FROM (
                SELECT wt.wiki_id,
                       wt.domain,
                       wt.time_id,
                       pv.pageviews AS pageviews
                  FROM (
                        SELECT w.wiki_id,
                               w.domain,
                               DATE(pt.time_id) AS time_id
                          FROM statsdb_mart.dimension_wikis w
                          STRAIGHT_JOIN statsdb_etl.etl_period_times pt
                            ON pt.period_id = 1
                           AND pt.time_id BETWEEN DATE_SUB(now(), INTERVAL 1 WEEK)
                                              AND now()
                         WHERE w.hub_name = 'Entertainment'
                       ) wt
                  LEFT JOIN rollup_wiki_pageviews pv
                    ON pv.period_id = 1
                   AND pv.time_id = wt.time_id
                   AND pv.wiki_id = wt.wiki_id
               ) pv
          LEFT JOIN rollup_wiki_provider_views vv
            ON vv.period_id = 1
           AND vv.time_id = pv.time_id
           AND vv.wiki_id = pv.wiki_id
         GROUP BY pv.wiki_id,
                  pv.time_id
       ) sub
 GROUP BY sub.wiki_id
 ORDER BY SUM(sub.views) DESC
 LIMIT 100


