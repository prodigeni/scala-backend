DROP TABLE IF EXISTS etl_globals;

CREATE TABLE etl_globals (
    setting  VARCHAR(255) NOT NULL,
    value    VARCHAR(255) NOT NULL,
    PRIMARY KEY (setting)
) ENGINE=InnoDB;

INSERT INTO etl_globals VALUES ('EnableLogLoading', '1');

COMMIT;

