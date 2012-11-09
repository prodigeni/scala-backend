CREATE TABLE statsdb.fact_trackingevent_events (
    source          ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id         INTEGER UNSIGNED NOT NULL,
    event_id        INTEGER UNSIGNED NOT NULL,
    event_ts        DATETIME NOT NULL,
    event_type      VARCHAR(255),
    beacon          CHAR(10),
    wiki_id         INTEGER,
    user_id         INTEGER,
    namespace_id    INTEGER,
    article_id      INTEGER,
    ga_category     VARCHAR(255),
    ga_action       VARCHAR(255),
    ga_label        VARCHAR(255),
    ga_value        INTEGER,
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (event_ts) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
);

