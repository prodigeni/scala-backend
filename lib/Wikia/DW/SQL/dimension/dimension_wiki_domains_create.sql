CREATE TABLE [schema].[table] (
    wiki_id    INTEGER NOT NULL,
    domain     VARCHAR(255),
    PRIMARY KEY (wiki_id, domain)
) ENGINE=InnoDB;
