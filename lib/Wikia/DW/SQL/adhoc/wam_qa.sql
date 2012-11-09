SELECT sub.time_id,
       sub.wam_rank_diff,
       COUNT(1) AS wikis
  FROM (
        SELECT t1.time_id,
               CAST(t1.wam_rank AS SIGNED) - CAST(t2.wam_rank AS SIGNED) AS wam_rank_diff
          FROM fact_wam_scores t1
          LEFT JOIN fact_wam_scores t2
            ON t2.wiki_id = t1.wiki_id
           AND t2.time_id = DATE_SUB(t1.time_id, INTERVAL 1 DAY)
         WHERE t1.time_id = '2012-06-08'
       ) sub
 GROUP BY sub.time_id,
          sub.wam_rank_diff
 ORDER BY COUNT(1) DESC
 LIMIT 40;

SELECT t1.*
  FROM fact_wam_scores t1
  LEFT JOIN fact_wam_scores t2
    ON t2.wiki_id = t1.wiki_id
   AND t2.time_id = DATE_SUB(t1.time_id, INTERVAL 1 DAY)
 WHERE t1.time_id = '2012-06-08'
   AND t2.wam_rank = 923

