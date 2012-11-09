DROP TABLE IF EXISTS statsdb_etl.etl_period_times;

CREATE TABLE statsdb_etl.etl_period_times (
    period_id        SMALLINT UNSIGNED NOT NULL,
    time_id          DATETIME,
    begin_time       DATETIME,
    end_time         DATETIME,
    epoch_time_id    INTEGER,
    epoch_begin_time INTEGER,
    epoch_end_time   INTEGER,
    PRIMARY KEY (begin_time, end_time, period_id, time_id),
    INDEX period_times (period_id, time_id),
    INDEX period_epoch (period_id, epoch_time_id)
) ENGINE=InnoDB;


-- Daily
INSERT INTO statsdb_etl.etl_period_times
SELECT 1  AS period_id,
       dt AS time_id,
       dt AS begin_time,
       DATE_ADD(dt, INTERVAL 1 DAY) AS end_time,
       UNIX_TIMESTAMP(dt) AS epoch_time_id,
       UNIX_TIMESTAMP(dt) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD(dt, INTERVAL 1 DAY)) AS epoch_end_time
  FROM statsdb_etl.etl_dates;

-- Weekly
INSERT INTO statsdb_etl.etl_period_times
SELECT 2  AS period_id,
       dt AS time_id,
       dt AS begin_time,
       DATE_ADD(dt, INTERVAL 7 DAY) AS end_time,
       UNIX_TIMESTAMP(dt) AS epoch_time_id,
       UNIX_TIMESTAMP(dt) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD(dt, INTERVAL 7 DAY)) AS epoch_end_time
  FROM statsdb_etl.etl_dates
 WHERE dow = 'Sun';

-- Monthly
INSERT INTO statsdb_etl.etl_period_times
SELECT 3  AS period_id,
       dt AS time_id,
       dt AS begin_time,
       DATE_ADD(dt, INTERVAL 1 MONTH) AS end_time,
       UNIX_TIMESTAMP(dt) AS epoch_time_id,
       UNIX_TIMESTAMP(dt) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD(dt, INTERVAL 1 MONTH)) AS epoch_end_time
  FROM statsdb_etl.etl_dates
 WHERE dt < DATE('2020-01-01')
   AND DATE_FORMAT(dt, '%d') = 1;

-- Quarterly
INSERT INTO statsdb_etl.etl_period_times
SELECT 4  AS period_id,
       dt AS time_id,
       dt AS begin_time,
       DATE_ADD(dt, INTERVAL 3 MONTH) AS end_time,
       UNIX_TIMESTAMP(dt) AS epoch_time_id,
       UNIX_TIMESTAMP(dt) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD(dt, INTERVAL 3 MONTH)) AS epoch_end_time
  FROM statsdb_etl.etl_dates
 WHERE dt < DATE('2020-01-01')
   AND DATE_FORMAT(dt, '%d') = 1
   AND DATE_FORMAT(dt, '%m') IN (1,4,7,10);

-- Yearly
INSERT INTO statsdb_etl.etl_period_times
SELECT 5  AS period_id,
       dt AS time_id,
       dt AS begin_time,
       DATE_ADD(dt, INTERVAL 1 YEAR) AS end_time,
       UNIX_TIMESTAMP(dt) AS epoch_time_id,
       UNIX_TIMESTAMP(dt) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD(dt, INTERVAL 1 YEAR)) AS epoch_end_time
  FROM statsdb_etl.etl_dates
 WHERE dt < DATE('2020-01-01')
   AND DATE_FORMAT(dt, '%d') = 1
   AND DATE_FORMAT(dt, '%m') = 1;

-- 15 Minute
INSERT INTO statsdb_etl.etl_period_times
SELECT 15 AS period_id,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE) AS time_id,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE) AS begin_time,
       DATE_ADD('2004-01-01', INTERVAL id*15 MINUTE)     AS end_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE)) AS epoch_time_id,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE)) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL id*15 MINUTE))     AS epoch_end_time
  FROM statsdb_etl.etl_ids
 WHERE DATE_ADD('2004-01-01', INTERVAL id*15 MINUTE) <= DATE('2020-01-01');

-- 60 Minute
INSERT INTO statsdb_etl.etl_period_times
SELECT 60 AS period_id,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*60 MINUTE) AS time_id,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*60 MINUTE) AS begin_time,
       DATE_ADD('2004-01-01', INTERVAL id*60 MINUTE)     AS end_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*60 MINUTE)) AS epoch_time_id,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*60 MINUTE)) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL id*60 MINUTE))     AS epoch_end_time
  FROM statsdb_etl.etl_ids
 WHERE DATE_ADD('2004-01-01', INTERVAL id*60 MINUTE) <= DATE('2020-01-01');

-- Rolling 7 Day (Every 1 Day)
INSERT INTO statsdb_etl.etl_period_times
SELECT 1007 AS period_id,
       dt   AS time_id,
       DATE_SUB(dt, INTERVAL 7 DAY) AS begin_time,
       dt   AS end_time,
       UNIX_TIMESTAMP(dt)   AS epoch_time_id,
       UNIX_TIMESTAMP(DATE_SUB(dt, INTERVAL 7 DAY)) AS epoch_begin_time,
       UNIX_TIMESTAMP(dt)   AS epoch_end_time
  FROM statsdb_etl.etl_dates;

-- Rolling 28 Day (Every 1 Day)
INSERT INTO statsdb_etl.etl_period_times
SELECT 1028 AS period_id,
       dt   AS time_id,
       DATE_SUB(dt, INTERVAL 28 DAY) AS begin_time,
       dt   AS end_time,
       UNIX_TIMESTAMP(dt)   AS epoch_time_id,
       UNIX_TIMESTAMP(DATE_SUB(dt, INTERVAL 28 DAY)) AS epoch_begin_time,
       UNIX_TIMESTAMP(dt)   AS epoch_end_time
  FROM statsdb_etl.etl_dates;

-- Rolling 24 Hour (Every 15 Minutes)
INSERT INTO statsdb_etl.etl_period_times
SELECT 10024 AS period_id,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE) AS time_id,
       DATE_SUB(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE), INTERVAL 1 DAY) AS begin_time,
       DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE) AS end_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE)) AS epoch_time_id,
       UNIX_TIMESTAMP(DATE_SUB(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE), INTERVAL 1 DAY)) AS epoch_begin_time,
       UNIX_TIMESTAMP(DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE)) AS epoch_end_time
  FROM statsdb_etl.etl_ids
 WHERE DATE_ADD('2004-01-01', INTERVAL (id-1)*15 MINUTE) <= DATE('2020-01-01');

COMMIT;
