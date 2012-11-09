CREATE TEMPORARY TABLE [schema].[table] (
    wiki_id         INTEGER UNSIGNED NOT NULL,
    name            VARCHAR(255),
    media_type      ENUM('UNKNOWN','BITMAP','DRAWING','AUDIO','VIDEO','MULTIMEDIA','OFFICE','TEXT','EXECUTABLE','ARCHIVE') DEFAULT NULL,
    PRIMARY KEY (wiki_id, name, media_type)
) ENGINE=InnoDB;

