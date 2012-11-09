CREATE TABLE statsdb.rollup_hub_beacon_pageviews (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    hub_id        TINYINT UNSIGNED,
    beacon        CHAR(10),
    pageviews     INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, hub_id, beacon)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

