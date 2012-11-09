CREATE TABLE statsdb.fact_addriver_events (
    source        ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id       INTEGER UNSIGNED NOT NULL,
    event_id      INTEGER UNSIGNED NOT NULL,
    event_ts      DATETIME NOT NULL,
    event_type    VARCHAR(255),
    beacon        CHAR(10),
    position      VARCHAR(10),
    ip            INTEGER UNSIGNED,
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (event_ts) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
);

