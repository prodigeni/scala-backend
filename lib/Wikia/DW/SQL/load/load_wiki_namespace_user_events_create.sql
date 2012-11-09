CREATE TABLE statsdb_mart.load_wiki_namespace_user_events (
    period_id     SMALLINT UNSIGNED NOT NULL,
    time_id       DATETIME NOT NULL,
    wiki_id       INTEGER UNSIGNED NOT NULL,
    namespace_id  INTEGER UNSIGNED NOT NULL,
    user_id       INTEGER UNSIGNED NOT NULL,
    new_creates   INTEGER UNSIGNED,
    new_edits     INTEGER UNSIGNED,
    new_deletes   INTEGER UNSIGNED,
    new_undeletes INTEGER UNSIGNED,
    PRIMARY KEY (time_id, period_id, wiki_id, namespace_id, user_id),
    INDEX (time_id, period_id, user_id, wiki_id, namespace_id)
) ENGINE=InnoDB

