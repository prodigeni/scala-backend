INSERT INTO statsdb_mart.rollup_wiki_article_pageviews (
    period_id,
    time_id,
    wiki_id,
    namespace_id,
    article_id,
    pageviews
)
SELECT period_id,
       time_id,
       wiki_id,
       namespace_id,
       article_id,
       new_pageviews
  FROM statsdb_mart.load_wiki_article_pageviews
    ON DUPLICATE KEY UPDATE pageviews = pageviews + new_pageviews

