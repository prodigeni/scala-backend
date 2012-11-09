CREATE TABLE statsdb.rollup_wiki_visit_metrics (
    period_id           SMALLINT UNSIGNED NOT NULL,
    time_id             DATETIME NOT NULL,
    wiki_id             INTEGER UNSIGNED NOT NULL,
    visits              INTEGER UNSIGNED,
    visitors            INTEGER UNSIGNED,
    pageviews_per_visit FLOAT(10,4),
    PRIMARY KEY (period_id, time_id, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
