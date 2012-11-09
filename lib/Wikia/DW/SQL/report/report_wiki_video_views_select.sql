SELECT sub.wiki_id,
       MAX(sub.domain)    AS domain,
       MAX(sub.hub_name)  AS hub,
       SUM(sub.pageviews) AS pageviews,
       SUM(sub.views)     AS total_video_views,
       SUM(sub.premium_views) AS premium_video_views
  FROM (
        SELECT pv.wiki_id,
               MAX(pv.domain)   AS domain,
               MAX(pv.hub_name) AS hub_name,
               pv.time_id,
               IFNULL(MAX(pv.pageviews),0) AS pageviews,
               SUM(vv.views) AS views,
               SUM(CASE WHEN vv.provider IN ('ign','screenplay') THEN vv.views ELSE 0 END) AS premium_views
          FROM (
                SELECT wt.wiki_id,
                       wt.domain,
                       wt.hub_name,
                       wt.time_id,
                       pv.pageviews AS pageviews
                  FROM (
                        SELECT w.wiki_id,
                               w.domain,
                               w.hub_name,
                               DATE(pt.time_id) AS time_id
                          FROM statsdb_mart.dimension_wikis w
                          STRAIGHT_JOIN statsdb_etl.etl_period_times pt
                            ON pt.period_id = 1 
                           AND pt.time_id = TIMESTAMP('$time_id')
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
HAVING SUM(sub.views) > 0 
ORDER BY SUM(sub.views) DESC

