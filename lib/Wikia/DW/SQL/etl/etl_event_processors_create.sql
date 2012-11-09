CREATE TABLE etl_event_processors (
  event_type varchar(255) NOT NULL,
  processor varchar(255) NOT NULL,
  PRIMARY KEY (event_type,processor)
) ENGINE=InnoDB;

INSERT INTO etl_event_processors VALUES
    ('api',                  'ApiProcessor'),
    ('create',               'EventProcessor'),
    ('default',              'DefaultProcessor'),
    ('delete',               'EventProcessor'),
    ('edit',                 'EventProcessor'),
    ('pageview',             'PageviewProcessor'),
    ('search_click',         'SearchProcessor'),
    ('search_start',         'SearchProcessor'),
    ('search_start_gomatch', 'SearchProcessor'),
    ('search_start_nomatch', 'SearchProcessor'),
    ('search_start_suggest', 'SearchProcessor'),
    ('undelete',             'EventProcessor');

COMMIT;

