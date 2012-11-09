CREATE TABLE statsdb_mart.rollup_wiki_video_views (
    period_id    SMALLINT UNSIGNED NOT NULL,
    time_id      DATETIME NOT NULL,
    wiki_id      INTEGER UNSIGNED NOT NULL,
    video_title  VARCHAR(255),
    views        INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, video_title),
    INDEX wiki_period_time (wiki_id, period_id, time_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)

