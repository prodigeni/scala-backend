CREATE TABLE [schema].[table] (
    wiki_id          INTEGER NOT NULL,
    variable_name    VARCHAR(255),
    variable_value   VARCHAR(255),
    PRIMARY KEY (wiki_id, variable_name)
) ENGINE=InnoDB;
