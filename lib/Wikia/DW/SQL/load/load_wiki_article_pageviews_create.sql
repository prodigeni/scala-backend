CREATE TABLE statsdb_mart.load_wiki_article_pageviews (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    wiki_id       INTEGER UNSIGNED NOT NULL,
    namespace_id  INTEGER UNSIGNED NOT NULL,
    article_id    INTEGER UNSIGNED NOT NULL,
    new_pageviews INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, namespace_id, article_id)
) ENGINE=InnoDB

