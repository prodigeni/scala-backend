DROP TABLE IF EXISTS statsdb_etl.etl_table_updates;

CREATE TABLE statsdb_etl.etl_table_updates (
    table_name    VARCHAR(255) NOT NULL,
    updated_at    DATETIME NOT NULL,
    period_id     SMALLINT NOT NULL,
    file_id       INTEGER UNSIGNED NOT NULL,
    first_ts      DATETIME,
    last_ts       DATETIME,
    duration      INTEGER,
    PRIMARY KEY (table_name, updated_at, period_id, file_id)
) ENGINE=InnoDB;

