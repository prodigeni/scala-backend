DROP TABLE IF EXISTS statsdb_etl.etl_periods;

CREATE TABLE statsdb_etl.etl_periods (
    period_id    SMALLINT UNSIGNED NOT NULL,
    description  VARCHAR(255),
    time_id      VARCHAR(255),
    etl_ids      VARCHAR(255),
    PRIMARY KEY (period_id)
) ENGINE=InnoDB;

INSERT INTO statsdb_etl.etl_periods VALUES
    (    1, 'Daily',     'DATE([ts])', 1),
    (    2, 'Weekly',    'DATE_SUB(DATE([ts]), INTERVAL DAYOFWEEK([ts])-1 DAY)', 1),
    (    3, 'Monthly',   'DATE(DATE_FORMAT([ts], \'%Y-%m-01\'))', 1),
    (    4, 'Quarterly', 'DATE_SUB(DATE(DATE_FORMAT([ts], \'%Y-%m-01\')), INTERVAL (MONTH([ts]) + 2) % 3 MONTH)', 1),
    (    5, 'Yearly',    'DATE(DATE_FORMAT([ts], \'%Y-01-01\'))', 1),
    (   15, '15 Minute', 'DATE_SUB(DATE_SUB([ts], INTERVAL MINUTE([ts]) % 15 MINUTE), INTERVAL SECOND([ts]) SECOND)', 1),
    (   60, '60 Minute', 'TIMESTAMP(DATE_FORMAT([ts], \'%Y-%m-%d %H:00:00\'))', 1),
    ( 1007, 'Rolling 7 Day (Every Day)',  'DATE_ADD(DATE([ts]), INTERVAL (ids.id-1) DAY)', 7),
    ( 1028, 'Rolling 28 Day (Every Day)', 'DATE_ADD(DATE([ts]), INTERVAL (ids.id-1) DAY)', 28),
    (10024, 'Rolling 24 Hours (Every 15 Minutes)',
            'DATE_ADD(DATE_ADD([ts], INTERVAL 900 - ((SECOND([ts]) + MINUTE([ts])*60) % 900) SECOND), INTERVAL (ids.id-1)*15 MINUTE)', 96)
;

COMMIT;

