CREATE TABLE statsdb.rollup_wiki_search_referrers (
    period_id         SMALLINT UNSIGNED NOT NULL,
    time_id           DATETIME NOT NULL,
    wiki_id           INTEGER UNSIGNED,
    search_domain_id  INTEGER UNSIGNED,
    search_term_id    INTEGER UNSIGNED,
    pageviews         INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, wiki_id, search_domain_id, search_term_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

