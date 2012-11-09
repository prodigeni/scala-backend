DROP TABLE IF EXISTS statsdb_etl.etl_ids;

CREATE TABLE statsdb_etl.etl_ids (
    id  INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

LOAD DATA LOCAL INFILE 'etl_ids.dat' INTO TABLE statsdb_etl.etl_ids (id);

COMMIT;

