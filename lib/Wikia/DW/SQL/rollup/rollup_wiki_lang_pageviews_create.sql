CREATE TABLE statsdb.rollup_wiki_lang_pageviews (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    lang         VARCHAR(8),
    pageviews    INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, lang)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

