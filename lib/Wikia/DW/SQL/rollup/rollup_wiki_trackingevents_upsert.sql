INSERT INTO statsdb.rollup_wiki_trackingevents (
    period_id,
    time_id,
    wiki_id,
    ga_category,
    ga_action,
    ga_label,
    ga_value,
    events
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       wiki_id,
       ga_category,
       ga_action,
       ga_label,
       ga_value,
       @new_events := COUNT(1) AS new_events
  FROM statsdb.fact_trackingevent_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id,
          ga_category,
          ga_action,
          ga_label,
          ga_value
    ON DUPLICATE KEY UPDATE events = events + @new_events

