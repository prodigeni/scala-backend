CREATE TABLE rollup_wiki_user_geo_events (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER NOT NULL,
    user_id      INTEGER NOT NULL,
    country_code CHAR(2) NOT NULL,
    region       CHAR(2),
    city         VARCHAR(50),
    creates      INTEGER UNSIGNED,
    edits        INTEGER UNSIGNED,
    deletes      INTEGER UNSIGNED,
    undeletes    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, user_id, country_code, region, city),
    INDEX (time_id, period_id, city, region, country_code, wiki_id, user_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

