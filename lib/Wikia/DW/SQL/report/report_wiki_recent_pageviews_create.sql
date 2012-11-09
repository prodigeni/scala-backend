CREATE TABLE statsdb_mart.report_wiki_recent_pageviews (
    wiki_id         INTEGER UNSIGNED NOT NULL,
    hub_name        VARCHAR(255),
    lang            VARCHAR(255),
    pageviews_7day  INTEGER UNSIGNED NOT NULL DEFAULT 0,
    pageviews_30day INTEGER UNSIGNED NOT NULL DEFAULT 0,
    pageviews_90day INTEGER UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (wiki_id),
    INDEX hub_lang (hub_name, lang),
    INDEX lang_hub (lang, hub_name)
) ENGINE=InnoDB;

