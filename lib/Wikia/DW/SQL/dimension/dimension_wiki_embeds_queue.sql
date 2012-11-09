INSERT INTO qdup.qdup_jobs (
    class,
    priority,
    status,
    args
)
SELECT 'Wikia::DW::ETL::Job::DimensionWikiEmbeds' AS class,
       95        AS priority,
       'WAITING' AS status,
       CONCAT('{ "wiki_id":', w.wiki_id, ', "dbname":"', dbname, '" }') AS args
  FROM (
--        SELECT DISTINCT e.wiki_id
--          FROM fact_embedchange_events e
--         WHERE e.event_ts >= DATE_SUB(now(), INTERVAL (8.5*60) MINUTE)
--         UNION
        SELECT wiki_id FROM lookup_video_wikis
         ) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 ORDER BY w.wiki_id
