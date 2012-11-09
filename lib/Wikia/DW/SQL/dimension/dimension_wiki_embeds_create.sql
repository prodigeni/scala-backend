CREATE TEMPORARY TABLE [schema].[table] (
    wiki_id         INTEGER UNSIGNED NOT NULL,
    article_id      INTEGER UNSIGNED NOT NULL,
    video_title     varchar(255) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
    added_at        datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
    added_by        INTEGER UNSIGNED NOT NULL DEFAULT '0',
    duration        INTEGER UNSIGNED NOT NULL DEFAULT '0',
    premium         TINYINT NOT NULL DEFAULT '0',
    hdfile          TINYINT NOT NULL DEFAULT '0',
    removed         TINYINT NOT NULL DEFAULT '0',
    views_30day     INTEGER UNSIGNED DEFAULT '0',
    views_total     INTEGER UNSIGNED DEFAULT '0',
    PRIMARY KEY (wiki_id, article_id, video_title)
) ENGINE=InnoDB;
