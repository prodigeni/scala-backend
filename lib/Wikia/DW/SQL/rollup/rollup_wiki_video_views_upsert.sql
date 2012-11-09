INSERT INTO statsdb_mart.rollup_wiki_video_views (
    period_id,
    time_id,
    wiki_id,
    video_title,
    views
)
SELECT period_id,
       time_id,
       wiki_id,
       video_title,
       new_views
  FROM statsdb_mart.load_wiki_video_views
    ON DUPLICATE KEY UPDATE views = views + new_views

