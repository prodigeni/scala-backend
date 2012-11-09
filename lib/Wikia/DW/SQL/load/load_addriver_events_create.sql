CREATE TABLE [TABLENAME] (
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

