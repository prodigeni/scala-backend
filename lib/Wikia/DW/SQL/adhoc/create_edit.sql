SELECT 'event' AS source,
       UNIX_TIMESTAMP(TIMESTAMP(r.rev_timestamp)) + (900 - UNIX_TIMESTAMP(TIMESTAMP(r.rev_timestamp)) % 900) AS file_id,
       null AS event_id,
       TIMESTAMP(r.rev_timestamp) AS event_ts,
       CASE WHEN rev_parent_id = 0 THEN 'create' ELSE 'edit' END AS event_type,
       null AS beacon,
       null AS wiki_id,
       r.rev_user AS user_id,
       p.page_namespace AS namespace_id,
       p.page_id AS article_id
  FROM revision r
  LEFT JOIN page p
    ON p.page_id = r.rev_page
 WHERE r.rev_timestamp >= 20120209000000
   AND r.rev_timestamp <  20120210000000
 ORDER BY r.rev_timestamp
