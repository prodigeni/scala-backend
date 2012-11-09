CREATE TABLE [schema].[table] (
    wiki_id          INTEGER UNSIGNED NOT NULL,
    dbname           VARCHAR(64),
    sitename         VARCHAR(255),
    url              VARCHAR(255),
    domain           VARCHAR(255),
    founding_user_id INTEGER UNSIGNED,
    public           TINYINT UNSIGNED,
    lang             VARCHAR(8),
    lang_id          SMALLINT UNSIGNED,
    hub_id           TINYINT UNSIGNED,
    hub_name         VARCHAR(255),
    created_at       DATETIME,
    rank             SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (wiki_id)
) ENGINE=InnoDB;

