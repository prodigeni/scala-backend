CREATE TABLE [TABLENAME] (
    source            ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id           INTEGER UNSIGNED NOT NULL,
    event_id          INTEGER UNSIGNED NOT NULL,
    event_ts          DATETIME NOT NULL,
    event_type        VARCHAR(255),
    referer_domain    VARCHAR(255),
    referer_path      TEXT,
    search_domain_id  SMALLINT UNSIGNED NOT NULL,
    search_term       VARCHAR(255),
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB

