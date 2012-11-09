DROP TABLE IF EXISTS statsdb_etl.etl_job_time_executions;

CREATE TABLE statsdb_etl.etl_job_time_executions (
    job_id         INTEGER UNSIGNED NOT NULL,
    time_id        DATETIME NOT NULL,
    executed_at    DATETIME,
    status         ENUM('RUNNING', 'COMPLETE', 'FAILED', 'SKIP'),
    duration       MEDIUMINT UNSIGNED,
    worker_id      VARCHAR(255),
    stdout         TEXT,
    stderr         TEXT,
    PRIMARY KEY (job_id, time_id, executed_at)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS statsdb_etl.etl_job_time_status;

CREATE TABLE statsdb_etl.etl_job_time_status (
    job_id           INTEGER UNSIGNED NOT NULL,
    time_id          DATETIME NOT NULL,
    last_executed_at DATETIME,
    status           ENUM('RUNNING', 'COMPLETE', 'FAILED', 'SKIP'),
    duration         MEDIUMINT UNSIGNED,
    worker_id        VARCHAR(255),
    stdout           TEXT,
    stderr           TEXT,
    PRIMARY KEY (job_id, time_id),
    INDEX time_job (time_id, job_id)
) ENGINE=InnoDB;

