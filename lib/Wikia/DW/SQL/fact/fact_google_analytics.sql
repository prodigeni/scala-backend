CREATE TABLE statsdb.fact_google_analytics (
    time_id       DATETIME,
    pageviews     INTEGER,
    pageviews_adj INTEGER,
    PRIMARY KEY (time_id)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE '../adhoc/google_analytics_wikia.csv' INTO TABLE fact_google_analytics FIELDS TERMINATED BY ',' IGNORE 1 LINES (@time_id, pageviews) SET time_id = STR_TO_DATE(@time_id, '%m/%d/%y')

COMMIT;

