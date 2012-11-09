CREATE TABLE statsdb_mart.rollup_wiki_namespace_user_events (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    namespace_id INTEGER UNSIGNED NOT NULL,
    user_id      INTEGER UNSIGNED NOT NULL,
    creates      INTEGER UNSIGNED,
    edits        INTEGER UNSIGNED,
    deletes      INTEGER UNSIGNED,
    undeletes    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, namespace_id, user_id),
    INDEX (time_id, period_id, user_id, wiki_id, namespace_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

