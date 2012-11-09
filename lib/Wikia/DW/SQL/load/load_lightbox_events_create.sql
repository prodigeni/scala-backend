CREATE TABLE [TABLENAME] (
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
    title           VARCHAR(255),
    provider        VARCHAR(255),
    click_source    VARCHAR(255),
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB

