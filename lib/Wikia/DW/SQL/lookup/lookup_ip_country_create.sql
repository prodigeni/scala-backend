DROP TABLE IF EXISTS statsdb.lookup_ip_country;

CREATE TABLE statsdb.lookup_ip_country (
  id           INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  ip_poly      POLYGON NOT NULL,
  ip_from      INTEGER UNSIGNED NOT NULL,
  ip_to        INTEGER UNSIGNED NOT NULL,
  country_code CHAR(2) NOT NULL,
  region       CHAR(2),
  city         VARCHAR(50),
  postal_code  CHAR(6) NOT NULL,
  latitude     DECIMAL(7,4),
  longitude    DECIMAL(7,4),
  dma_code     INTEGER,
  area_code    INTEGER,
  PRIMARY KEY (id),
  SPATIAL INDEX (ip_poly)
) ENGINE=MyISAM;

