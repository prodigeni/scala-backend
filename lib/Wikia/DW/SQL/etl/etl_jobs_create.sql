DROP TABLE IF EXISTS statsdb_etl.etl_jobs;

CREATE TABLE statsdb_etl.etl_jobs (
    job_id          INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
    job_class       VARCHAR(255),
    job_params      VARCHAR(255),
    job_period_id   INTEGER UNSIGNED,
    job_lag         TINYINT,
    job_queue       VARCHAR(255),
    job_status      ENUM('ENABLED','DISABLED') NOT NULL DEFAULT 'DISABLED',
    job_begin_time  DATETIME,    
    job_end_time    DATETIME,    
    created_at      DATETIME,
    updated_at      TIMESTAMP,
    PRIMARY KEY (job_class, job_params, job_period_id, job_lag),
    UNIQUE INDEX id_idx (job_id),
    INDEX (job_queue)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE 'etl_jobs.csv' INTO TABLE statsdb_etl.etl_jobs FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;

COMMIT;

