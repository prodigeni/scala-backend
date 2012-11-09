INSERT INTO qdup.qdup_jobs (
    class,
    status,
    priority,
    args
)
SELECT 'Wikia::DW::ETL::Job::DimensionWikiImageCounts' AS class,
       'WAITING' AS status,
       80        AS priority,
       CONCAT('{ "wiki_id":', w.wiki_id, ', "dbname":"', w.dbname, '" }') AS args
  FROM (
        SELECT DISTINCT wiki_id
          FROM fact_event_events 
         WHERE event_ts >= DATE_SUB(now(), INTERVAL (8.5*60) MINUTE)
           AND event_type = 'create'
           AND namespace_id = 6
       ) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 ORDER BY RAND()
