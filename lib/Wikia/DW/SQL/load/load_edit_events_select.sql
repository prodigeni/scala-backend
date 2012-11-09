SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       wiki_id,
       namespace_id,
       article_id,
       user_id,
       COUNT(CASE WHEN e.event_type = 'create'   THEN 1 ELSE null END) AS new_creates,
       COUNT(CASE WHEN e.event_type = 'edit'     THEN 1 ELSE null END) AS new_edits,
       COUNT(CASE WHEN e.event_type = 'delete'   THEN 1 ELSE null END) AS new_deletes,
       COUNT(CASE WHEN e.event_type = 'undelete' THEN 1 ELSE null END) AS new_undeletes
  FROM statsdb.fact_event_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id,
          namespace_id,
          article_id,
          user_id
