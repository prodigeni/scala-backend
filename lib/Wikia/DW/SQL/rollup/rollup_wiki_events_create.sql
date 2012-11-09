CREATE TABLE statsdb.rollup_wiki_events (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    creates      INTEGER UNSIGNED,
    edits        INTEGER UNSIGNED,
    deletes      INTEGER UNSIGNED,
    undeletes    INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, wiki_id),
    INDEX (wiki_id, period_id, time_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

