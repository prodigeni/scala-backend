DROP TABLE IF EXISTS statsdb_etl.etl_files;

CREATE TABLE statsdb_etl.etl_files (
    source     ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id    INTEGER UNSIGNED NOT NULL,
    s3_file    VARCHAR(255),
    loaded_at  DATETIME,
    PRIMARY KEY (source, file_id)
) ENGINE=InnoDB;

