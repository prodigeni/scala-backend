CREATE TABLE [schema].[table] (
    wiki_id         INTEGER UNSIGNED NOT NULL,
    namespace_id    INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (wiki_id, namespace_id)
) ENGINE=InnoDB;

