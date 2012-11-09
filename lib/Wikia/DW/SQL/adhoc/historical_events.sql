SELECT TIMESTAMP(l.log_timestamp) AS event_ts,
       CASE WHEN l.log_action = 'restore' THEN 'undelete' ELSE 'delete' END AS event_type,
       null AS wiki_id,
       (SELECT a.ar_user
          FROM archive a
         WHERE a.ar_namespace = l.log_namespace
           AND a.ar_title = l.log_title
           AND a.ar_timestamp <= l.log_timestamp
         ORDER BY a.ar_timestamp DESC
         LIMIT 1) AS user_id,
       l.log_namespace  AS namespace_id,
       (SELECT a.ar_page_id
          FROM archive a
         WHERE a.ar_namespace = l.log_namespace
           AND a.ar_title = l.log_title
           AND a.ar_timestamp <= l.log_timestamp
         ORDER BY a.ar_timestamp DESC
         LIMIT 1) AS archive_article_id,
       (SELECT r.rev_page
          FROM page p
         WHERE p.page_namespace = l.log_namespace
           AND p.page_title = l.log_title
           AND p.ar_timestamp <= l.log_timestamp
         ORDER BY a.ar_timestamp DESC
         LIMIT 1) AS article_id,
       CASE WHEN l.log_action = 'restore' THEN 0 ELSE l.log_id END AS log_id,
       CASE WHEN l.log_action = 'restore' THEN null ELSE 0 END AS rev_id,
       TIMESTAMP(l.log_timestamp) AS rev_timestamp
  FROM logging l
 WHERE l.log_type IN ('delete')
   AND l.log_timestamp > 20120524000000
 UNION ALL
SELECT TIMESTAMP(r.rev_timestamp) AS event_ts,
       CASE WHEN r.rev_parent_id = 0 OR r.rev_parent_id IS NULL THEN 'create' ELSE 'edit' END AS event_type,
       null AS wiki_id,
       r.rev_user AS user_id,
       p.page_namespace AS namespace_id,
       p.page_id AS article_id,
       0 AS log_id,
       r.rev_id AS rev_id,
       TIMESTAMP(r.rev_timestamp) AS rev_timestamp
  FROM revision r
  LEFT JOIN page p
    ON p.page_id = r.rev_page
 WHERE r.rev_timestamp > 20120524000000
 UNION ALL
SELECT TIMESTAMP(a.ar_timestamp) AS event_ts,
       CASE WHEN a.ar_parent_id = 0 OR a.ar_parent_id IS NULL THEN 'create' ELSE 'edit' END AS event_type,
       null AS wiki_id,
       a.ar_user AS user_id,
       a.ar_namespace AS namespace_id,
       a.ar_page_id AS article_id,
       0 AS log_id,
       a.ar_rev_id AS rev_id,
       TIMESTAMP(a.ar_timestamp) AS rev_timestamp
  FROM archive a
 WHERE a.ar_timestamp > 20120524000000
 ORDER BY event_type,
          event_ts

