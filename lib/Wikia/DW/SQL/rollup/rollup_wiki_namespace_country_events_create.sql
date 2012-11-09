CREATE TABLE statsdb.rollup_wiki_namespace_country_events (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER NOT NULL,
    namespace_id INTEGER NOT NULL,
    country_code CHAR(2) NOT NULL,
    creates      INTEGER UNSIGNED,
    edits        INTEGER UNSIGNED,
    deletes      INTEGER UNSIGNED,
    undeletes    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, namespace_id, country_code),
    INDEX (time_id, period_id, country_code, wiki_id, namespace_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

