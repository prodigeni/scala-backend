SELECT [wiki_id]  AS wiki_id,
       il.il_from AS article_id,
       v.video_title,
       v.added_at,
       v.added_by,
       v.duration,
       v.premium,
       v.hdfile,
       v.removed,
       v.views_30day,
       v.views_total
  FROM `[dbname]`.imagelinks il
  JOIN `[dbname]`.video_info v
    ON v.video_title = il.il_to
