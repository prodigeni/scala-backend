EXPLAIN
SELECT sub2.*
  FROM (
        SELECT w.wiki_id,
               w.hub_id         AS wiki_cat_id,
               w.lang_id        AS wiki_lang_id,
               DATE(sub.time_id)  AS stats_date,
               COUNT(sub.user_id) AS users_all,
               COUNT(CASE WHEN sub.content_edits > 0 THEN sub.user_id ELSE null END) AS users_content_ns,
               COUNT(CASE WHEN sub.edits >= 5        THEN sub.user_id ELSE null END) AS users_5times,
               COUNT(CASE WHEN sub.edits >= 100      THEN sub.user_id ELSE null END) AS users_100times
          FROM (
                SELECT r.time_id,
                       r.wiki_id,
                       r.user_id,
                       SUM(r.creates + r.edits + r.deletes + r.undeletes) AS edits,
                       SUM(CASE WHEN r.namespace_id =   0 THEN r.creates + r.edits + r.deletes + r.undeletes ELSE null END) AS content_edits,
                       SUM(CASE WHEN r.namespace_id =   6 THEN r.creates + r.edits + r.deletes + r.undeletes ELSE null END) AS image_uploads,
                       SUM(CASE WHEN r.namespace_id = 400 THEN r.creates + r.edits + r.deletes + r.undeletes ELSE null END) AS video_uploads
                  FROM statsdb_etl.etl_period_times pt
                  JOIN rollup_wiki_namespace_user_events r
                    ON r.time_id = pt.time_id
                   AND r.period_id = 3
                   AND r.user_id != 0
                 WHERE pt.period_id = 3
                   AND pt.time_id BETWEEN '2011-12-01'
                                      AND '2011-12-02'
                 GROUP BY r.time_id,
                          r.wiki_id,
                          r.user_id
               ) sub
          JOIN dimension_wikis w
            ON w.wiki_id = sub.wiki_id
         GROUP BY w.wiki_id,
                  sub.time_id
      ) sub2
 JOIN (
       SELECT r.wiki_id,
              r.time_id,
              COUNT(DISTINCT r.namespace_id, r.article_id) AS articles
         FROM statsdb_etl.etl_period_times pt
         JOIN rollup_edit_events r
           ON r.time_id = pt.time_id
          AND r.period_id = 3
          AND r.user_id != 0
        WHERE pt.period_id = 3
          AND pt.time_id BETWEEN '2011-12-01'
                             AND '2011-12-02'
        GROUP BY r.time_id,
                 r.wiki_id
      ) articles
    ON articles.wiki_id = sub2.wiki_id
   AND articles.time_id = sub2.time_id

