DROP TABLE statsdb_tmp.visit_pageviews;
CREATE TABLE statsdb_tmp.visit_pageviews (
    source          ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id         INTEGER UNSIGNED NOT NULL,
    event_id        INTEGER UNSIGNED NOT NULL,
    event_ts        DATETIME,
    event_type      VARCHAR(255),
    beacon          CHAR(10),
    wiki_id         INTEGER,
    user_id         INTEGER,
    namespace_id    INTEGER,
    article_id      INTEGER,
    visit_id        BINARY(16),
    visitor_id      BINARY(16),
    PRIMARY KEY (event_ts, visitor_id, visit_id, event_id),
    INDEX (event_id)
) ENGINE=InnoDB;
INSERT INTO statsdb_tmp.visit_pageviews
SELECT *
  FROM fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.visit_id IS NOT NULL;
COMMIT;
