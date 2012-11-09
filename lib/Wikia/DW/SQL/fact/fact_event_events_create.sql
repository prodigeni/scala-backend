CREATE TABLE statsdb.fact_event_events (
    source          ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id         INTEGER UNSIGNED NOT NULL,
    event_id        INTEGER UNSIGNED NOT NULL,
    event_ts        DATETIME NOT NULL,
    event_type      VARCHAR(255),
    beacon          CHAR(10),
    wiki_id         INTEGER,
    user_id         INTEGER,
    namespace_id    SMALLINT,
    article_id      INTEGER,
    ip              INTEGER UNSIGNED NOT NULL DEFAULT '0',
    is_content      ENUM('N','Y') DEFAULT 'N',
    is_redirect     ENUM('N','Y') DEFAULT 'N',
    user_is_bot     ENUM('N','Y') DEFAULT 'N',
    log_id          INTEGER UNSIGNED NOT NULL DEFAULT '0',
    media_type      TINYINT UNSIGNED NOT NULL DEFAULT '0',
    rev_id          INTEGER UNSIGNED NOT NULL,
    rev_size        MEDIUMINT UNSIGNED NOT NULL DEFAULT '0',
    rev_timestamp   DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',
    total_words     INTEGER UNSIGNED NOT NULL DEFAULT '0',
    image_links     INTEGER UNSIGNED NOT NULL DEFAULT '0',
    video_links     INTEGER UNSIGNED NOT NULL DEFAULT '0',
    wiki_cat_id     TINYINT UNSIGNED NOT NULL DEFAULT '0',
    wiki_lang_id    SMALLINT UNSIGNED NOT NULL DEFAULT '0',
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (event_ts) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
);

