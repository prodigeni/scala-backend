DROP TABLE IF EXISTS statsdb_etl.etl_job_dependencies;

CREATE TABLE statsdb_etl.etl_job_dependencies (
    job_id            INTEGER UNSIGNED NOT NULL,
    depends_job_id    INTEGER UNSIGNED NOT NULL,
    wait_for_complete ENUM('Y','N'),
    begin_time        DATETIME,
    end_time          DATETIME,
    created_at        DATETIME,
    updated_at        DATETIME,
    PRIMARY KEY (job_id, depends_job_id),
    INDEX depends_job (depends_job_id, job_id)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE 'etl_job_dependencies.csv' INTO TABLE statsdb_etl.etl_job_dependencies FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;

COMMIT;

