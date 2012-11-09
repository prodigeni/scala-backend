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

DELIMITER |

CREATE TRIGGER statsdb_etl.after_status_insert AFTER INSERT ON statsdb_etl.etl_job_time_status
  FOR EACH ROW BEGIN
    IF NEW.last_executed_at IS NOT NULL AND NEW.status != 'RUNNING' THEN
        INSERT INTO statsdb_etl.etl_job_time_executions (
            job_id,
            time_id,
            executed_at,
            status,
            duration,
            worker_id,
            stdout,
            stderr
        ) VALUES (
            NEW.job_id,
            NEW.time_id,
            NEW.last_executed_at,
            NEW.status,
            NEW.duration,
            NEW.worker_id,
            NEW.stdout,
            NEW.stderr
        );
    END IF;
  END;
|

CREATE TRIGGER statsdb_etl.after_status_update AFTER UPDATE ON statsdb_etl.etl_job_time_status
  FOR EACH ROW BEGIN
    IF NEW.last_executed_at IS NOT NULL AND NEW.status != 'RUNNING' THEN
        INSERT INTO statsdb_etl.etl_job_time_executions (
            job_id,
            time_id,
            executed_at,
            status,
            duration,
            worker_id,
            stdout,
            stderr
        ) VALUES (
            NEW.job_id,
            NEW.time_id,
            NEW.last_executed_at,
            NEW.status,
            NEW.duration,
            NEW.worker_id,
            NEW.stdout,
            NEW.stderr
        );
    END IF;
  END;
|

DELIMITER ;
