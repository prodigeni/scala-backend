DROP TABLE IF EXISTS statsdb_etl.etl_job_queues;

CREATE TABLE statsdb_etl.etl_job_queues (
    queue     VARCHAR(255) NOT NULL,
    status    ENUM('UP', 'DOWN') NOT NULL DEFAULT 'DOWN',
    workers   TINYINT UNSIGNED,
    PRIMARY KEY (queue)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE 'etl_job_queues.csv' INTO TABLE statsdb_etl.etl_job_queues FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;

COMMIT;

