CREATE TABLE statsdb.fact_wiki_mobile_pageviews (
    time_id                DATETIME,
    mobile_pv              INTEGER UNSIGNED,
    mobile_pv_per_visit    FLOAT,
    nonmobile_pv           INTEGER UNSIGNED,
    nonmobile_pv_per_visit FLOAT,
    PRIMARY KEY (time_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS (time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
);
