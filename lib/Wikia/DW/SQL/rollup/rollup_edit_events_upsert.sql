INSERT INTO statsdb_mart.rollup_edit_events (
    period_id,
    time_id,
    wiki_id,
    namespace_id,
    article_id,
    user_id,
    creates,
    edits,
    deletes,
    undeletes
)
SELECT period_id,
       time_id,
       wiki_id,
       namespace_id,
       article_id,
       user_id,
       new_creates,
       new_edits,
       new_deletes,
       new_undeletes
  FROM statsdb_mart.load_edit_events
    ON DUPLICATE KEY UPDATE creates   = creates   + new_creates,
                            edits     = edits     + new_edits,
                            deletes   = deletes   + new_deletes,
                            undeletes = undeletes + new_undeletes

