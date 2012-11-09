CREATE EXTERNAL TABLE onedot ( line STRING )
partitioned by (ds STRING)
LOCATION 's3://one_dot_archive/${DATE}';

ALTER TABLE onedot ADD PARTITION (ds='${DATE}') location 's3://one_dot_archive/${DATE}';

ALTER TABLE onedot RECOVER PARTITIONS; 

CREATE EXTERNAL TABLE onedot_data ( city INT, lang STRING, langid INT, dbname STRING, cl STRING, user INT, article INT, namespace INT, referrer STRING, beacon STRING, ts STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/onedot_data/${DATE}';

add file s3://wikia/odyssey/mapper.pl;

FROM onedot INSERT OVERWRITE TABLE onedot_data 
SELECT transform (line) USING 'perl mapper.pl' AS (city, lang, langid, dbname, cl, user, article, namespace, referrer, beacon, ts );

CREATE EXTERNAL TABLE odyssey_data ( beacon STRING, referrer STRING, city STRING, article STRING, user STRING, ts STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/data/${DATE}';

add file s3://wikia/odyssey/oddyssey_data.pl;

FROM (select * from onedot_data DISTRIBUTE BY beacon SORT BY ts) s
INSERT OVERWRITE TABLE odyssey_data
REDUCE * USING 'perl odyssey_data.pl' AS (referrer, beacon, city, article, user, ts);


