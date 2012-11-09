INSERT INTO statsdb.rollup_api_events (
    period_id,
    time_id,
    api_key,
    api_type,
    api_function,
    ip,
    wiki_id,
    events
)
SELECT @new_period_id    := [period_id] AS period_id,
       @new_time_id      := [time_id]   AS time_id,
       @new_api_key      := api_key,
       @new_api_type     := api_type,
       @new_api_function := api_function,
       @new_ip           := ip,
       @new_wiki_id      := wiki_id,
       @new_events       := COUNT(1) AS events
  FROM statsdb.fact_api_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          api_key,
          api_type,
          api_function,
          ip,
          wiki_id
    ON DUPLICATE KEY UPDATE events = events + @new_events

