CREATE TABLE statsdb.fact_referrer_events (
    source             ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id            INTEGER UNSIGNED NOT NULL,
    event_id           INTEGER UNSIGNED NOT NULL,
    event_ts           DATETIME NOT NULL,
    event_type         VARCHAR(255),
    referrer_domain_id INTEGER UNSIGNED,
    referrer_path      TEXT,
    search_domain_id   SMALLINT UNSIGNED,
    search_term_id     INTEGER UNSIGNED,
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (event_ts) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
);

