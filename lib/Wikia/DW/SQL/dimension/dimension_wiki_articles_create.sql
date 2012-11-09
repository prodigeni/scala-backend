CREATE TEMPORARY TABLE [schema].[table] (
    wiki_id         INTEGER UNSIGNED NOT NULL,
    namespace_id    INTEGER UNSIGNED NOT NULL,
    article_id      INTEGER UNSIGNED NOT NULL,
    title           VARCHAR(255),
    PRIMARY KEY (wiki_id, namespace_id, article_id)
) ENGINE=InnoDB;

