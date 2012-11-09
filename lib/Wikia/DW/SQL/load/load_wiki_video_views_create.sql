CREATE TABLE statsdb_mart.load_wiki_video_views (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    wiki_id       INTEGER UNSIGNED NOT NULL,
    video_title   VARCHAR(255),
    new_views INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, video_title)
) ENGINE=InnoDB

