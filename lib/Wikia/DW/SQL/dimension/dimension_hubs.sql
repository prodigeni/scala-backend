DROP TABLE IF EXISTS statsdb.dimension_hubs;

CREATE TABLE statsdb.dimension_hubs (
    hub_id     TINYINT UNSIGNED NOT NULL,
    hub_name   VARCHAR(255),
    wiki_id    INTEGER UNSIGNED,
    article_id INTEGER UNSIGNED,
    PRIMARY KEY (hub_id)
) ENGINE=InnoDB;

INSERT INTO statsdb.dimension_hubs VALUES 
    ( 1, 'Humor',           null, null),
    ( 2, 'Gaming',         80433, 3867),
    ( 3, 'Entertainment',  80433, 3876),
    ( 4, 'Wikia',          80433, 1461),
    ( 5, 'Toys',            null, null),
    ( 6, 'Food and Drink',  null, null),
    ( 8, 'Education',       null, null),
    ( 7, 'Travel',          null, null),
    ( 9, 'Lifestyle',      80433, 3875),
    (10, 'Finance',         null, null),
    (11, 'Politics',        null, null),
    (12, 'Technology',      null, null),
    (13, 'Science',         null, null),
    (14, 'Philosophy',      null, null),
    (15, 'Sports',          null, null),
    (16, 'Music',           null, null),
    (17, 'Creative',        null, null),
    (18, 'Auto',            null, null),
    (19, 'Green',           null, null),
    (20, 'Wikianswers',     null, null)
;

COMMIT;
