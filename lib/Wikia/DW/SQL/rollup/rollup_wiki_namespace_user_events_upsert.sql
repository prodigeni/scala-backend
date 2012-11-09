INSERT INTO statsdb_mart.rollup_wiki_namespace_user_events (
    period_id,
    time_id,
    wiki_id,
    namespace_id,
    user_id,
    creates,
    deletes,
    undeletes,
    edits
)
SELECT period_id,
       time_id,
       wiki_id,
       namespace_id,
       user_id,
       new_creates,
       new_deletes,
       new_undeletes,
       new_edits
  FROM statsdb_mart.load_wiki_namespace_user_events
    ON DUPLICATE KEY UPDATE creates   = creates   + new_creates,
                            deletes   = deletes   + new_deletes,
                            undeletes = undeletes + new_undeletes,
                            edits     = edits     + new_edits

