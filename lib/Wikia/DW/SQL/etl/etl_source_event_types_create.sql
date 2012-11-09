DROP TABLE IF EXISTS statsdb_etl.etl_source_event_types;

CREATE TABLE statsdb_etl.etl_source_event_types (
    source       ENUM('api', 'event', 'special', 'view') NOT NULL,
    event_type   VARCHAR(255) NOT NULL,
    PRIMARY KEY (source, event_type)
) ENGINE=InnoDB;

INSERT INTO statsdb_etl.etl_source_event_types VALUES
    ( 'api',     'api'      ),
    ( 'view',    'pageview' ),
    ( 'event',   'create'   ),
    ( 'event',   'delete'   ),
    ( 'event',   'edit'     ),
    ( 'event',   'undelete' ),
    ( 'special', 'search_click'         ),
    ( 'special', 'search_start'         ),
    ( 'special', 'search_start_gomatch' ),
    ( 'special', 'search_start_nomatch' ),
    ( 'special', 'search_start_suggest' );

COMMIT;

