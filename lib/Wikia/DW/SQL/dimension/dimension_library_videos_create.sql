CREATE TABLE [schema].[table] (
    video_title    VARCHAR(255),
    added_at       DATETIME NOT NULL,
    added_by       INTEGER UNSIGNED NOT NULL,
    duration       INTEGER UNSIGNED NOT NULL,
    premium        TINYINT,
    hdfile         TINYINT,
    removed        TINYINT,
    views_30day    INTEGER UNSIGNED,
    views_total    INTEGER UNSIGNED,
  PRIMARY KEY (video_title),
  KEY added_at (added_at,duration),
  KEY premium (premium,added_at),
  KEY hdfile (hdfile,added_at)
) ENGINE=InnoDB

