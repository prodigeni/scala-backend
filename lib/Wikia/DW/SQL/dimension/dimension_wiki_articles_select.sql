SELECT [wiki_id]      AS wiki_id,
       page_namespace AS namespace_id,
       page_id        AS article_id,
       page_title     AS title
  FROM `[dbname]`.page
