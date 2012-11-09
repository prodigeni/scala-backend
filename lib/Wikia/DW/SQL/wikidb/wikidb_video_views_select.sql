SELECT video_title, 
       SUM(CASE WHEN time_id >= DATE_SUB(DATE(now()), INTERVAL 29 DAY) THEN views ELSE 0 END) AS views_30day, 
       SUM(views) AS views_total 
  FROM rollup_wiki_video_views 
 WHERE period_id = 1 
   AND wiki_id = [wiki_id]
 GROUP BY video_title
