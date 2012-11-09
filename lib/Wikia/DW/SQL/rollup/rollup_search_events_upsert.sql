INSERT INTO statsdb.rollup_search_events (
    period_id,
    time_id,
    search_click,
    search_start,
    search_start_gomatch,
    search_start_match,
    search_start_nomatch,
    search_start_suggest
)
SELECT @new_period_id            := [period_id] AS period_id,
       @new_time_id              := [time_id]   AS time_id,
       @new_search_click         := COUNT(CASE WHEN e.event_type = 'search_click'         THEN 1 ELSE null END) AS search_click,
       @new_search_start         := COUNT(CASE WHEN e.event_type = 'search_start'         THEN 1 ELSE null END) AS search_start,
       @new_search_start_gomatch := COUNT(CASE WHEN e.event_type = 'search_start_gomatch' THEN 1 ELSE null END) AS search_start_gomatch,
       @new_search_start_match   := COUNT(CASE WHEN e.event_type = 'search_start_match'   THEN 1 ELSE null END) AS search_start_match,
       @new_search_start_nomatch := COUNT(CASE WHEN e.event_type = 'search_start_nomatch' THEN 1 ELSE null END) AS search_start_nomatch,
       @new_search_start_suggest := COUNT(CASE WHEN e.event_type = 'search_start_suggest' THEN 1 ELSE null END) AS search_start_suggest
  FROM statsdb.fact_search_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id
    ON DUPLICATE KEY UPDATE search_click         = search_click         + @new_search_click,
                            search_start         = search_start         + @new_search_start,
                            search_start_gomatch = search_start_gomatch + @new_search_start_gomatch,
                            search_start_match   = search_start_match   + @new_search_start_match,
                            search_start_nomatch = search_start_nomatch + @new_search_start_nomatch,
                            search_start_suggest = search_start_suggest + @new_search_start_suggest

