INSERT INTO statsdb.rollup_wiki_events (
    period_id,
    time_id,
    wiki_id,
    creates,
    deletes,
    undeletes,
    edits
)
SELECT @new_period_id := [period_id] AS period_id,
       @new_time_id   := [time_id]   AS time_id,
       @new_wiki_id   := wiki_id,
       @new_creates   := COUNT(CASE WHEN e.event_type = 'create'   THEN 1 ELSE null END) AS creates,
       @new_deletes   := COUNT(CASE WHEN e.event_type = 'delete'   THEN 1 ELSE null END) AS deletes,
       @new_undeletes := COUNT(CASE WHEN e.event_type = 'undelete' THEN 1 ELSE null END) AS undeletes,
       @new_edits     := COUNT(CASE WHEN e.event_type = 'edit'     THEN 1 ELSE null END) AS edits
  FROM statsdb.fact_event_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id
    ON DUPLICATE KEY UPDATE creates   = creates   + @new_creates,
                            deletes   = deletes   + @new_deletes,
                            undeletes = undeletes + @new_undeletes,
                            edits     = edits     + @new_edits

