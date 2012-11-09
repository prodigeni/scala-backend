CREATE TABLE statsdb_mart.rollup_edit_events (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    namespace_id INTEGER UNSIGNED NOT NULL,
    article_id   INTEGER UNSIGNED NOT NULL,
    user_id      INTEGER UNSIGNED NOT NULL,
    creates      INTEGER UNSIGNED,
    edits        INTEGER UNSIGNED,
    deletes      INTEGER UNSIGNED,
    undeletes    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, namespace_id, article_id, user_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

