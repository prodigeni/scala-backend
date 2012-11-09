CREATE TABLE [TABLENAME] (
    source          ENUM('api', 'event', 'special', 'view') NOT NULL,
    file_id         INTEGER UNSIGNED NOT NULL,
    event_id        INTEGER UNSIGNED NOT NULL,
    event_ts        DATETIME NOT NULL,
    event_type      VARCHAR(255),
#   wiki_id         INTEGER,      # Will use dimension_wiki_domains table (which is replicated from prod) to determine this from hostname. Will actually be stored in fact_api_events.wiki_id.
    host            VARCHAR(255), # eg: muppet.wikia.com - will be converted to wiki_id before storing in fact_api_events
    api_type        VARCHAR(255), # {LW, Core MW, Nirvana, Wikia Extensions to MW, Unknown}
    api_function    VARCHAR(255), # The function called (eg: categorymembers)
    request_method  VARCHAR(255), # {get, post, head, etc.}
    api_key         BINARY(16),
    ip              INT UNSIGNED,         # eg: 123.45.67.89 (use INET_ATON(dottedQuadIpHere) to convert to int and INET_ATON(theIntVersion) to convert back to dotted-quad notation).
    called_by_wikia TINYINT(1) DEFAULT 0, # (1 = yes, 0 = no, -1 = Unknown) - whether it was Wikia code using the API or not
    PRIMARY KEY (event_ts, event_id, source, file_id)
) ENGINE=InnoDB

