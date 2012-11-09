CREATE TABLE statsdb.report_wiki_video_views (
    time_id              DATETIME NOT NULL,
    wiki_id              INTEGER UNSIGNED NOT NULL,
    pageviews            INTEGER UNSIGNED NOT NULL,
    total_video_views    INTEGER UNSIGNED NOT NULL,
    premium_video_views  INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (time_id, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
