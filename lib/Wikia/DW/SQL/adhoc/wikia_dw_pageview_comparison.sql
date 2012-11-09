SELECT CASE WHEN IFNULL(dw.pageviews,0) > 1 THEN '2+' ELSE IFNULL(dw.pageviews,0) END AS "Data Warehouse Pageviews",
       COUNT(1) AS "Wikis"
  FROM statsdb.dimension_wikis w
  LEFT JOIN statsdb_tmp.page_views_dw_rollup dw       ON dw.wiki_id = w.wiki_id
  LEFT JOIN statsdb_tmp.page_views_wikia_rollup wikia ON wikia.wiki_id = w.wiki_id
 WHERE w.created_at < '2012-01-12'
   AND IFNULL(wikia.pageviews,0) IN (0,1)
 GROUP BY CASE WHEN IFNULL(dw.pageviews,0) > 1 THEN '2+' ELSE IFNULL(dw.pageviews,0) END
 ORDER BY 1, 2
