CREATE TABLE statsdb.rollup_trackingevents (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    namespace_id INTEGER UNSIGNED NOT NULL,
    article_id   INTEGER UNSIGNED NOT NULL,
    ga_category  VARCHAR(255),
    ga_action    VARCHAR(255),
    ga_label     VARCHAR(255),
    ga_value     INTEGER,
    events       INTEGER UNSIGNED,
    PRIMARY KEY (period_id, time_id, wiki_id, namespace_id, article_id, ga_category, ga_action, ga_label, ga_value)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
