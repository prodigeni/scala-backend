CREATE TABLE statsdb.report_wiki_video_embeds (
    time_id        DATETIME NOT NULL,
    wiki_id        INTEGER UNSIGNED NOT NULL,
    articles       INTEGER UNSIGNED NOT NULL,
    total_embeds   INTEGER UNSIGNED NOT NULL,
    premium_embeds INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (time_id, wiki_id)
) ENGINE=InnoDB
  PARTITION BY RANGE COLUMNS(time_id) (
    PARTITION p0 VALUES LESS THAN ('1970-01-01')
)
