INSERT INTO statsdb.rollup_wiki_searches (
    period_id,
    time_id,
    wiki_id,
    search_term,
    search_click,
    search_start,
    search_start_gomatch,
    search_start_nomatch,
    search_start_suggest,
    reciprocal_sum
)
SELECT @new_period_id            := [period_id] AS period_id,
       @new_time_id              := [time_id]   AS time_id,
       @new_wiki_id              := e.wiki_id,
       @new_search_term          := e.search_term,
       @new_search_click         := COUNT(CASE WHEN e.event_type = 'search_click'         THEN 1 ELSE null END) AS search_click,
       @new_search_start         := COUNT(CASE WHEN e.event_type = 'search_start'         THEN 1 ELSE null END) AS search_start,
       @new_search_start_gomatch := COUNT(CASE WHEN e.event_type = 'search_start_gomatch' THEN 1 ELSE null END) AS search_start_gomatch,
       @new_search_start_nomatch := COUNT(CASE WHEN e.event_type = 'search_start_nomatch' THEN 1 ELSE null END) AS search_start_nomatch,
       @new_search_start_suggest := COUNT(CASE WHEN e.event_type = 'search_start_suggest' THEN 1 ELSE null END) AS search_start_suggest,
       @new_reciprocal_sum       := SUM(CASE WHEN e.event_type = 'search_click' THEN 1/position ELSE 0 END)
  FROM statsdb.fact_search_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id,
          search_term
    ON DUPLICATE KEY UPDATE search_click         = search_click         + @new_search_click,
                            search_start         = search_start         + @new_search_start,
                            search_start_gomatch = search_start_gomatch + @new_search_start_gomatch,
                            search_start_nomatch = search_start_nomatch + @new_search_start_nomatch,
                            search_start_suggest = search_start_suggest + @new_search_start_suggest,
                            reciprocal_sum       = reciprocal_sum       + @new_reciprocal_sum

