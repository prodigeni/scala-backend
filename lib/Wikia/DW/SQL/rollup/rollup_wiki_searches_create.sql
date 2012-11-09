CREATE TABLE statsdb.rollup_wiki_searches (
    period_id               SMALLINT NOT NULL,
    time_id                 DATETIME NOT NULL,
    wiki_id                 INTEGER UNSIGNED NOT NULL,
    search_term             VARCHAR(255) NOT NULL,
    search_click            INTEGER,
    search_start            INTEGER,
    search_start_gomatch    INTEGER,
    search_start_nomatch    INTEGER,
    search_start_suggest    INTEGER,
    reciprocal_sum          FLOAT(12,4),
    PRIMARY KEY (period_id, time_id, wiki_id, search_term)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
