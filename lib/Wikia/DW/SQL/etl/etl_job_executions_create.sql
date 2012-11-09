DROP TABLE IF EXISTS statsdb_etl.etl_job_executions;

CREATE TABLE statsdb_etl.etl_job_executions (
    job_id         INTEGER UNSIGNED NOT NULL,
    time_id        DATETIME NOT NULL,
    executed_at    DATETIME,
    duration       SMALLINT UNSIGNED,
    status         ENUM('SUCCESS','FAIL'),
    PRIMARY KEY (job_id, time_id, executed_at)
) ENGINE=InnoDB;

