CREATE TABLE statsdb.dimension_specialpages (
    id      INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
    name    VARCHAR(128) NOT NULL,
    PRIMARY KEY (id),
    KEY name_idx (name)
) AUTO_INCREMENT=1000000000 ENGINE=InnoDB;
