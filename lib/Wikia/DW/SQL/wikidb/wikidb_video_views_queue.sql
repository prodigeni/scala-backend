INSERT INTO qdup.qdup_jobs (
    class,
    priority,
    status,
    args
)
SELECT 'Wikia::DW::ETL::Job::WikiDBVideoViews' AS class,
       50        AS priority,
       'WAITING' AS status,
       CONCAT('{ "wiki_id":', w.wiki_id, ', "dbname":"', dbname, '" }') AS args
  FROM (
        SELECT DISTINCT e.wiki_id
          FROM lookup_video_wikis w
          JOIN fact_lightbox_events e
            ON e.wiki_id = w.wiki_id
           AND e.event_ts >= DATE_SUB(now(), INTERVAL (5*60) MINUTE)
           AND e.ga_category = 'lightbox'
           AND e.ga_action   = 'view'
           AND e.ga_label   IN ('video', 'video-inline')
       ) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 ORDER BY w.wiki_id

