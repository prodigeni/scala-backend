CREATE TABLE [TABLENAME] (
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
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB

