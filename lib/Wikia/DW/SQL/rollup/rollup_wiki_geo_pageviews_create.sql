CREATE TABLE rollup_wiki_geo_pageviews (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    country_code CHAR(2) NOT NULL,
    region       CHAR(2),
    city         VARCHAR(50),
    pageviews    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

