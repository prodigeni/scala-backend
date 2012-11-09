DROP TABLE IF EXISTS statsdb_etl.etl_sources;

CREATE TABLE statsdb_etl.etl_sources (
    source  ENUM('api', 'event', 'special', 'view') NOT NULL,
    PRIMARY KEY (source)
) ENGINE=InnoDB;

INSERT INTO statsdb_etl.etl_sources VALUES 
    ('api'),
    ('event'),
    ('special'),
    ('view');

COMMIT;

