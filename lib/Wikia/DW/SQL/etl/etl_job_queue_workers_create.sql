DROP TABLE IF EXISTS statsdb_etl.etl_job_queue_workers;

CREATE TABLE statsdb_etl.etl_job_queue_workers (
    queue            VARCHAR(255),
    worker_id        VARCHAR(255),
    created_at       DATETIME,
    updated_at       TIMESTAMP,
    PRIMARY KEY (queue, worker_id)
) ENGINE=InnoDB;

