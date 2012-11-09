CREATE TABLE fact_wam_inputs (
    time_id            TIMESTAMP WITHOUT TIME ZONE,
    wiki_id            INTEGER,
    pageviews_28day    INTEGER,
    pageviews_7day     INTEGER,
    edits_28day        INTEGER,
    edits_7day         INTEGER,
    editors_28day      INTEGER,
    editors_7day       INTEGER,
    pageviews_28_to_15 INTEGER,
    pageviews_14_to_1  INTEGER,
    pageviews_8_to_5   INTEGER,
    pageviews_4_to_1   INTEGER,
    PRIMARY KEY (time_id, wiki_id)
);
