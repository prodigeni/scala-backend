CREATE TABLE dimension_search_terms (
    search_term_id    INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
    search_term       VARCHAR(255) NOT NULL,
    PRIMARY KEY (search_term_id),
    UNIQUE KEY search_terms_idx_uniq (search_term)
) ENGINE=InnoDB;

