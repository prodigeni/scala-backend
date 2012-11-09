CREATE TABLE statsdb.rollup_wiki_visits (
    period_id          SMALLINT UNSIGNED NOT NULL,
    time_id            DATETIME NOT NULL,
    wiki_id            INTEGER UNSIGNED NOT NULL,
    visit_id           BINARY(16) NOT NULL,
    visitor_id         BINARY(16),
    first_user_id      INTEGER UNSIGNED,
    first_namespace_id INTEGER UNSIGNED,
    first_article_id   INTEGER UNSIGNED,
    first_ts           DATETIME,
    last_user_id       INTEGER UNSIGNED,
    last_namespace_id  INTEGER UNSIGNED,
    last_article_id    INTEGER UNSIGNED,
    last_ts            DATETIME,
    pageviews          INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, wiki_id, visit_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

