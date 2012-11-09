INSERT INTO qdup.qdup_jobs (
    class,
    args
)
SELECT 'Wikia::DW::ETL::Job::DimensionWikiArticles' AS class,
       CONCAT('{ "wiki_id":', w.wiki_id, ', "dbname":"', dbname, '" }') AS args
  FROM (SELECT DISTINCT e.wiki_id
          FROM rollup_wiki_events e
         WHERE e.period_id = 1
           AND e.time_id >= now() - INTERVAL (8.5*60) MINUTE
         ORDER BY RAND()
       ) sub
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
