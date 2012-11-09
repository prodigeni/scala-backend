CREATE TABLE [schema].[table_name] (
  wiki_id       INTEGER UNSIGNED,
  user_id       INTEGER UNSIGNED,
  log_id        INTEGER UNSIGNED,
  event_type    TINYINT UNSIGNED DEFAULT '6',
  event_date    DATETIME,
  PRIMARY KEY (log_id),
  KEY wikilog (wiki_id,log_id),
  KEY users (user_id,wiki_id),
  KEY wiki_users (wiki_id,user_id),
  KEY event_date (event_date)
) ENGINE=InnoDB;

