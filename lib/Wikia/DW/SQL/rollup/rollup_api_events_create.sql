CREATE TABLE statsdb.rollup_api_events (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    api_key       BINARY(16) NOT NULL,
    api_type      VARCHAR(255),
    api_function  VARCHAR(255),
    ip            INTEGER UNSIGNED NOT NULL,
    wiki_id       INTEGER UNSIGNED NOT NULL,
    events        INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, api_key, api_type, api_function, ip, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

