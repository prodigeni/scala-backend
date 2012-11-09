DROP TABLE IF EXISTS fact_addriver_ab_treatments;

CREATE TABLE fact_addriver_ab_treatments (
    beacon             CHAR(10),
    treatment_group_id TINYINT,
    start_time         DATETIME,
    end_time           DATETIME,
    PRIMARY KEY (beacon, treatment_group_id)
) ENGINE=InnoDB;

INSERT INTO fact_addriver_ab_treatments (
    beacon,
    treatment_group_id,
    start_time,
    end_time
)
SELECT beacon,
       treatment_group_id,
       MIN(event_ts) AS start_time,
       MAX(event_ts) AS end_time
  FROM fact_ab_treatment_events
 WHERE treatment_group_id IN (1,2)
 GROUP BY beacon,
          treatment_group_id
HAVING MIN(event_ts) >= TIMESTAMP('2012-05-10 10:40:00');


DROP TABLE IF EXISTS fact_addriver_beacon_pageviews;

CREATE TABLE fact_addriver_beacon_pageviews (
    beacon    CHAR(10),
    event_ts  DATETIME,
    INDEX beacon_ts (beacon, event_ts)
) ENGINE=InnoDB;

INSERT INTO fact_addriver_beacon_pageviews (
    beacon,
    event_ts
)
SELECT pv.beacon,
       pv.event_ts
  FROM fact_addriver_ab_treatments ab
  JOIN fact_pageview_events pv
    ON pv.event_ts >= TIMESTAMP('2012-05-10 10:40:00')
   AND pv.event_ts <  TIMESTAMP('2012-05-16 00:00:00')
   AND pv.beacon    = ab.beacon;


DROP TABLE IF EXISTS rollup_addriver_beacon_pageviews;

CREATE TABLE rollup_addriver_beacon_pageviews (
    beacon       CHAR(10),
    pageviews    MEDIUMINT,
    PRIMARY KEY (beacon)
) ENGINE=InnoDB;

INSERT INTO rollup_addriver_beacon_pageviews (
    beacon,
    pageviews
)
SELECT ab.beacon,
       COUNT(pv.beacon) AS pageviews
  FROM fact_addriver_ab_treatments ab
  LEFT JOIN fact_addriver_beacon_pageviews pv
    ON pv.beacon = ab.beacon
   AND pv.event_ts >= ab.start_time
 GROUP BY ab.beacon;

