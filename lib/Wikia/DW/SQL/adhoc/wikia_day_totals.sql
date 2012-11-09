SELECT SUM(creates) AS total_creates,
       SUM(edits)   AS total_edits,
       SUM(deletes) AS total_deletes,
       SUM(undeletes) AS total_undeletes
  FROM (
        SELECT e.user_id,
               u.user_name,
               SUM(e.creates)   AS creates,
               SUM(e.edits)     AS edits,
               SUM(e.deletes)   AS deletes,
               SUM(e.undeletes) AS undeletes
          FROM rollup_wiki_user_events e
          JOIN dimension_wikia_users u
            ON u.user_id = e.user_id
         WHERE e.period_id = 15
           AND e.time_id BETWEEN '2011-12-16 08:00:00'
                             AND '2011-12-17 08:00:00'
         GROUP BY e.user_id
       ) sub

