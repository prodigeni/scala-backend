CREATE TABLE dimension_domains (
  domain_id         INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  domain            VARCHAR(255) NOT NULL,
  PRIMARY KEY (domain_id),
  UNIQUE KEY domains_idx_uniq (domain)
) ENGINE=InnoDB;

