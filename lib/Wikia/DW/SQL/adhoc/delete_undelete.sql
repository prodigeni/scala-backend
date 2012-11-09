SELECT 'event' AS source,
       UNIX_TIMESTAMP(TIMESTAMP(l.log_timestamp)) + (900 - UNIX_TIMESTAMP(TIMESTAMP(l.log_timestamp)) % 900) AS file_id,
       null AS event_id,
       CASE WHEN log_action = 'restore' THEN 'undelete' ELSE 'delete' END AS event_type,
       CASE WHEN l.log_type = 'move' THEN COALESCE(TIMESTAMP(a.ar_timestamp), TIMESTAMP(l.log_timestamp)) ELSE TIMESTAMP(l.log_timestamp) END AS event_ts,
       null            AS beacon,
       null            AS wiki_id,
       CASE WHEN l.log_type = 'move' THEN COALESCE(a.ar_user, l.log_user) ELSE l.log_user END AS user_id,
       l.log_namespace AS l_namespace_id,
       l.log_page      AS l_page_id,
       a.ar_page_id    AS a_page_id,
       l.*
  FROM logging l
  LEFT JOIN archive a
    ON a.ar_title = l.log_title
 WHERE l.log_type IN ('delete', 'move')
 ORDER BY CASE WHEN l.log_type = 'move' THEN COALESCE(TIMESTAMP(a.ar_timestamp), TIMESTAMP(l.log_timestamp)) ELSE TIMESTAMP(l.log_timestamp) END
