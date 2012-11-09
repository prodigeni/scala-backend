DROP TABLE IF EXISTS statsdb_etl.etl_file_loads;

CREATE TABLE statsdb_etl.etl_file_loads (
    source        ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id       INTEGER UNSIGNED NOT NULL,
    load_table    VARCHAR(255) NOT NULL,
    load_ts       DATETIME NOT NULL,
    loaded        INTEGER,
    rejected      INTEGER,
    rowcount      INTEGER,
    min_event_ts  DATETIME,
    max_event_ts  DATETIME,
    PRIMARY KEY (source, file_id, load_table, load_ts)
) ENGINE=InnoDB;

