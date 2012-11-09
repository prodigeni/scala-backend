CREATE TABLE statsdb.rollup_search_events (
    period_id               SMALLINT NOT NULL,
    time_id                 DATETIME NOT NULL,
    search_click            INTEGER,
    search_start            INTEGER,
    search_start_gomatch    INTEGER,
    search_start_match      INTEGER,
    search_start_nomatch    INTEGER,
    search_start_suggest    INTEGER,
    PRIMARY KEY (period_id, time_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
