DROP TABLE IF EXISTS statsdb_etl.etl_job_status;

CREATE TABLE statsdb_etl.etl_job_status (
    job_id           INTEGER UNSIGNED NOT NULL,
    time_id          DATETIME NOT NULL,
    last_executed_at DATETIME,
    status           ENUM('SUCCESS','FAIL'),
    PRIMARY KEY (job_id, time_id)
) ENGINE=InnoDB;

