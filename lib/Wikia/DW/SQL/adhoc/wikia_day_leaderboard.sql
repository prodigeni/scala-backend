SELECT e.user_id,
       u.user_name,
       u.user_email,
       SUM(e.creates)   AS creates,
       SUM(e.edits)     AS edits,
       SUM(e.deletes)   AS deletes,
       SUM(e.undeletes) AS undeletes,
       COUNT(DISTINCT e.wiki_id) AS wikis
  FROM rollup_wiki_user_events e
  JOIN dimension_wikia_users u
    ON u.user_id = e.user_id
   AND u.verified = TRUE
 WHERE e.period_id = 15
   AND e.time_id BETWEEN '2011-12-16 00:00:00'
                     AND '2011-12-17 08:00:00'
 GROUP BY e.user_id
 ORDER BY SUM(e.edits) DESC





