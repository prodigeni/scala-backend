CREATE TABLE statsdb.rollup_wiki_provider_views (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    provider     VARCHAR(255),
    views        INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, provider)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)


