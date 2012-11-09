CREATE TABLE statsdb.rollup_wiki_beacon_pageviews (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    wiki_id       INTEGER UNSIGNED,
    beacon        CHAR(10),
    pageviews     INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, wiki_id, beacon)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

