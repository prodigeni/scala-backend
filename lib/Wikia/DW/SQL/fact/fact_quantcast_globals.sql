CREATE TABLE statsdb.fact_quantcast_globals (
    time_id              DATETIME NOT NULL,
    wiki_id              INTEGER UNSIGNED NOT NULL,
    pageviews            INTEGER,
    visits               INTEGER,
    cookies              INTEGER,
    cookies_7day         INTEGER,
    cookies_30day        INTEGER,
    people               INTEGER,
    people_7day          INTEGER,
    people_30day         INTEGER,
    pageviews_per_person DECIMAL(6,2),
    visits_per_person    DECIMAL(6,2),
    PRIMARY KEY (time_id, wiki_id)
    KEY (wiki_id, time_id)
) ENGINE=InnoDB;

