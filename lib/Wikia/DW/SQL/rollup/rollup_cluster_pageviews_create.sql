CREATE TABLE statsdb.rollup_cluster_pageviews (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    cluster      VARCHAR(255),
    pageviews    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, cluster)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

