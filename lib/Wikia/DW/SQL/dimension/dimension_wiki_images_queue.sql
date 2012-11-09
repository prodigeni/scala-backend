INSERT INTO qdup.qdup_jobs (
    class,
    status,
    args
)
SELECT 'Wikia::DW::ETL::Job::DimensionWikiImages' AS class,
       'WAITING' AS status,
       CONCAT('{ "wiki_id":', w.wiki_id, ', "dbname":"', dbname, '" }') AS args
  FROM (SELECT wiki_id FROM dimension_wikis) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 ORDER BY w.wiki_id
