CREATE TEMPORARY TABLE [table].[table] (
    wiki_id     INTEGER UNSIGNED NOT NULL,
    archive     INTEGER UNSIGNED,
    audio       INTEGER UNSIGNED,
    bitmap      INTEGER UNSIGNED,
    drawing     INTEGER UNSIGNED,
    executable  INTEGER UNSIGNED,
    multimedia  INTEGER UNSIGNED,
    office      INTEGER UNSIGNED,
    text        INTEGER UNSIGNED,
    unknown     INTEGER UNSIGNED,
    video       INTEGER UNSIGNED,
    PRIMARY KEY (wiki_id)
) ENGINE=Innodb;

