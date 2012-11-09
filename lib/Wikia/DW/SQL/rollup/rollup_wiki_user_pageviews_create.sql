CREATE TABLE statsdb.rollup_wiki_user_pageviews (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    user_id      INTEGER UNSIGNED NOT NULL,
    pageviews    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, user_id),
    INDEX (time_id, period_id, user_id, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

