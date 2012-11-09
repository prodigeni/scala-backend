CREATE TABLE statsdb.rollup_event_events (
    period_id   INTEGER UNSIGNED NOT NULL,
    time_id     DATETIME NOT NULL,
    creates     INTEGER UNSIGNED,
    deletes     INTEGER UNSIGNED,
    undeletes   INTEGER UNSIGNED,
    edits       INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
