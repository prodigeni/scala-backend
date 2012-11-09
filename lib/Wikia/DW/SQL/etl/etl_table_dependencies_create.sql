DROP TABLE IF EXISTS statsdb_etl.etl_table_dependencies;

CREATE TABLE statsdb_etl.etl_table_dependencies (
    table_name  VARCHAR(255) NOT NULL DEFAULT '',
    depends_on  VARCHAR(255) NOT NULL DEFAULT '',
    PRIMARY KEY (table_name, depends_on)
) ENGINE=InnoDB
