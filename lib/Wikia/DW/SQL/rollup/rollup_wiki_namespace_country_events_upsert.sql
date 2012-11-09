INSERT INTO statsdb.rollup_wiki_namespace_country_events (
    period_id,
    time_id,
    wiki_id,
    namespace_id,
    country_code,
    creates,
    deletes,
    undeletes,
    edits
)
SELECT @new_period_id := [period_id] AS period_id,
       @new_time_id   := [time_id]   AS time_id,
       @new_wiki_id   := e.wiki_id,
       @new_namespace_id := e.namespace_id,
       @new_country_code := ip.country_code,
       @new_creates   := COUNT(CASE WHEN e.event_type = 'create'   THEN 1 ELSE null END) AS creates,
       @new_deletes   := COUNT(CASE WHEN e.event_type = 'delete'   THEN 1 ELSE null END) AS deletes,
       @new_undeletes := COUNT(CASE WHEN e.event_type = 'undelete' THEN 1 ELSE null END) AS undeletes,
       @new_edits     := COUNT(CASE WHEN e.event_type = 'edit'     THEN 1 ELSE null END) AS edits
  FROM statsdb.fact_event_events e
  JOIN statsdb.lookup_ip_country ip
    ON MBRCONTAINS(ip_poly, POINTFROMWKB(POINT(e.ip,0)))
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          e.wiki_id,
          e.namespace_id,
          country_code
    ON DUPLICATE KEY UPDATE creates   = creates   + @new_creates,
                            deletes   = deletes   + @new_deletes,
                            undeletes = undeletes + @new_undeletes,
                            edits     = edits     + @new_edits
