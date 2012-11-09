INSERT INTO statsdb.rollup_wiki_visits (
    period_id,
    time_id,
    wiki_id,
    visit_id,
    visitor_id,
    first_user_id,
    first_namespace_id,
    first_article_id,
    first_ts,
    last_user_id,
    last_namespace_id,
    last_article_id,
    last_ts,
    pageviews
)
SELECT @new_period_id  := [period_id] AS period_id,
       @new_time_id    := [time_id] AS time_id,
       @new_wiki_id    := e.wiki_id,
       @new_visit_id   := e.visit_id,
       @new_visitor_id := e.visitor_id,
       @new_first_user_id      := e.user_id      AS first_user_id,
       @new_first_namespace_id := e.namespace_id AS first_namespace_id,
       @new_first_article_id   := e.article_id   AS first_article_id,
       @new_first_ts           := e.event_ts     AS first_ts,
       @new_last_user_id      := e.user_id       AS last_user_id,
       @new_last_namespace_id := e.namespace_id  AS last_namespace_id,
       @new_last_article_id   := e.article_id    AS last_article_id,
       @new_last_ts           := e.event_ts      AS last_ts,
       @new_pageviews := 1 
  FROM fact_pageview_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.visit_id IS NOT NULL
 ORDER BY e.event_ts,
          e.event_id
    ON DUPLICATE KEY UPDATE visitor_id        = COALESCE(@new_visitor_id, statsdb.rollup_wiki_visits.visitor_id),
                            last_user_id      = COALESCE(@new_last_user_id, statsdb.rollup_wiki_visits.last_user_id),
                            last_namespace_id = @new_last_namespace_id,
                            last_article_id   = @new_last_article_id,
                            last_ts           = @new_last_ts,
                            pageviews         = pageviews + 1

