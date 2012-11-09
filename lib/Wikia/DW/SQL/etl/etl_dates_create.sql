DROP TABLE IF EXISTS statsdb_etl.etl_dates;

CREATE TABLE statsdb_etl.etl_dates (
    id         SMALLINT UNSIGNED NOT NULL,
    dt         DATE,
    date_id    INTEGER,
    dow        CHAR(3),
    PRIMARY KEY (id),
    UNIQUE KEY (dt),
    UNIQUE KEY (date_id)
) ENGINE=InnoDB;

INSERT INTO statsdb_etl.etl_dates
SELECT id,
       DATE_ADD('2004-01-01', INTERVAL id-1 DAY) AS dt,
       DATE_FORMAT(DATE_ADD('2004-01-01', INTERVAL id-1 DAY), '%Y%m%d') AS date_id,
       DATE_FORMAT(DATE_ADD('2004-01-01', INTERVAL id-1 DAY), '%W') AS dow
  FROM statsdb_etl.etl_ids
 WHERE DATE_ADD('2004-01-01', INTERVAL id-1 DAY) <= DATE('2020-01-01');

COMMIT;

