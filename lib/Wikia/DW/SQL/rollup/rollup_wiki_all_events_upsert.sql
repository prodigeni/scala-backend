INSERT INTO statsdb.rollup_wiki_all_events (
    period_id,
    time_id,
    wiki_id,

    main_creates,
    main_edits,
    main_deletes,
    main_undeletes,

    template_creates,
    template_edits,
    template_deletes,
    template_undeletes,

    file_creates,
    file_edits,
    file_deletes,
    file_undeletes,

    user_talk_creates,
    user_talk_edits,
    user_talk_deletes,
    user_talk_undeletes,

    talk_creates,
    talk_edits,
    talk_deletes,
    talk_undeletes,

    user_creates,
    user_edits,
    user_deletes,
    user_undeletes,

    category_creates,
    category_edits,
    category_deletes,
    category_undeletes,

    blog_article_talk_creates,
    blog_article_talk_edits,
    blog_article_talk_deletes,
    blog_article_talk_undeletes,

    project_creates,
    project_edits,
    project_deletes,
    project_undeletes,

    mediawiki_main_creates,
    mediawiki_main_edits,
    mediawiki_main_deletes,
    mediawiki_main_undeletes
)
SELECT @new_period_id := [period_id] AS period_id,
       @new_time_id   := [time_id]   AS time_id,
       @new_wiki_id   := wiki_id,
       @new_main_creates                := COUNT(CASE WHEN e.namespace_id =   0 AND e.event_type = 'create'   THEN 1 ELSE null END) AS main_creates,
       @new_main_edits                  := COUNT(CASE WHEN e.namespace_id =   0 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS main_edits,
       @new_main_deletes                := COUNT(CASE WHEN e.namespace_id =   0 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS main_deletes,
       @new_main_undeletes              := COUNT(CASE WHEN e.namespace_id =   0 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS main_undeletes,

       @new_template_creates            := COUNT(CASE WHEN e.namespace_id =  10 AND e.event_type = 'create'   THEN 1 ELSE null END) AS template_creates,
       @new_template_edits              := COUNT(CASE WHEN e.namespace_id =  10 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS template_edits,
       @new_template_deletes            := COUNT(CASE WHEN e.namespace_id =  10 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS template_deletes,
       @new_template_undeletes          := COUNT(CASE WHEN e.namespace_id =  10 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS template_undeletes,

       @new_file_creates                := COUNT(CASE WHEN e.namespace_id =   6 AND e.event_type = 'create'   THEN 1 ELSE null END) AS file_creates,
       @new_file_edits                  := COUNT(CASE WHEN e.namespace_id =   6 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS file_edits,
       @new_file_deletes                := COUNT(CASE WHEN e.namespace_id =   6 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS file_deletes,
       @new_file_undeletes              := COUNT(CASE WHEN e.namespace_id =   6 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS file_undeletes,

       @new_user_talk_creates           := COUNT(CASE WHEN e.namespace_id =   3 AND e.event_type = 'create'   THEN 1 ELSE null END) AS user_talk_creates,
       @new_user_talk_edits             := COUNT(CASE WHEN e.namespace_id =   3 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS user_talk_edits,
       @new_user_talk_deletes           := COUNT(CASE WHEN e.namespace_id =   3 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS user_talk_deletes,
       @new_user_talk_undeletes         := COUNT(CASE WHEN e.namespace_id =   3 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS user_talk_undeletes,

       @new_talk_creates                := COUNT(CASE WHEN e.namespace_id =   1 AND e.event_type = 'create'   THEN 1 ELSE null END) AS talk_creates,
       @new_talk_edits                  := COUNT(CASE WHEN e.namespace_id =   1 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS talk_edits,
       @new_talk_deletes                := COUNT(CASE WHEN e.namespace_id =   1 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS talk_deletes,
       @new_talk_undeletes              := COUNT(CASE WHEN e.namespace_id =   1 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS talk_undeletes,

       @new_user_creates                := COUNT(CASE WHEN e.namespace_id =   2 AND e.event_type = 'create'   THEN 1 ELSE null END) AS user_creates,
       @new_user_edits                  := COUNT(CASE WHEN e.namespace_id =   2 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS user_edits,
       @new_user_deletes                := COUNT(CASE WHEN e.namespace_id =   2 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS user_deletes,
       @new_user_undeletes              := COUNT(CASE WHEN e.namespace_id =   2 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS user_undeletes,

       @new_category_creates            := COUNT(CASE WHEN e.namespace_id =  14 AND e.event_type = 'create'   THEN 1 ELSE null END) AS category_creates,
       @new_category_edits              := COUNT(CASE WHEN e.namespace_id =  14 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS category_edits,
       @new_category_deletes            := COUNT(CASE WHEN e.namespace_id =  14 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS category_deletes,
       @new_category_undeletes          := COUNT(CASE WHEN e.namespace_id =  14 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS category_undeletes,

       @new_blog_article_talk_creates   := COUNT(CASE WHEN e.namespace_id = 501 AND e.event_type = 'create'   THEN 1 ELSE null END) AS blog_article_talk_creates,
       @new_blog_article_talk_edits     := COUNT(CASE WHEN e.namespace_id = 501 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS blog_article_talk_edits,
       @new_blog_article_talk_deletes   := COUNT(CASE WHEN e.namespace_id = 501 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS blog_article_talk_deletes,
       @new_blog_article_talk_undeletes := COUNT(CASE WHEN e.namespace_id = 501 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS blog_article_talk_undeletes,

       @new_project_creates             := COUNT(CASE WHEN e.namespace_id =   4 AND e.event_type = 'create'   THEN 1 ELSE null END) AS project_creates,
       @new_project_edits               := COUNT(CASE WHEN e.namespace_id =   4 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS project_edits,
       @new_project_deletes             := COUNT(CASE WHEN e.namespace_id =   4 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS project_deletes,
       @new_project_undeletes           := COUNT(CASE WHEN e.namespace_id =   4 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS project_undeletes,

       @new_mediawiki_main_creates      := COUNT(CASE WHEN e.namespace_id =   8 AND e.event_type = 'create'   THEN 1 ELSE null END) AS mediawiki_main_creates,
       @new_mediawiki_main_edits        := COUNT(CASE WHEN e.namespace_id =   8 AND e.event_type = 'edit'     THEN 1 ELSE null END) AS mediawiki_main_edits,
       @new_mediawiki_main_deletes      := COUNT(CASE WHEN e.namespace_id =   8 AND e.event_type = 'delete'   THEN 1 ELSE null END) AS mediawiki_main_deletes,
       @new_mediawiki_main_undeletes    := COUNT(CASE WHEN e.namespace_id =   8 AND e.event_type = 'undelete' THEN 1 ELSE null END) AS mediawiki_main_undeletes
  FROM statsdb.fact_event_events e
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
 GROUP BY period_id,
          time_id,
          wiki_id
    ON DUPLICATE KEY UPDATE main_creates   = main_creates   + @new_main_creates,
                            main_edits     = main_edits     + @new_main_edits,
                            main_deletes   = main_deletes   + @new_main_deletes,
                            main_undeletes = main_undeletes + @new_main_undeletes,

                            template_creates   = template_creates   + @new_template_creates,
                            template_edits     = template_edits     + @new_template_edits,
                            template_deletes   = template_deletes   + @new_template_deletes,
                            template_undeletes = template_undeletes + @new_template_undeletes,

                            file_creates   = file_creates   + @new_file_creates,
                            file_edits     = file_edits     + @new_file_edits,
                            file_deletes   = file_deletes   + @new_file_deletes,
                            file_undeletes = file_undeletes + @new_file_undeletes,

                            user_talk_creates   = user_talk_creates   + @new_user_talk_creates,
                            user_talk_edits     = user_talk_edits     + @new_user_talk_edits,
                            user_talk_deletes   = user_talk_deletes   + @new_user_talk_deletes,
                            user_talk_undeletes = user_talk_undeletes + @new_user_talk_undeletes,

                            talk_creates   = talk_creates   + @new_talk_creates,
                            talk_edits     = talk_edits     + @new_talk_edits,
                            talk_deletes   = talk_deletes   + @new_talk_deletes,
                            talk_undeletes = talk_undeletes + @new_talk_undeletes,

                            user_creates   = user_creates   + @new_user_creates,
                            user_edits     = user_edits     + @new_user_edits,
                            user_deletes   = user_deletes   + @new_user_deletes,
                            user_undeletes = user_undeletes + @new_user_undeletes,

                            category_creates   = category_creates   + @new_category_creates,
                            category_edits     = category_edits     + @new_category_edits,
                            category_deletes   = category_deletes   + @new_category_deletes,
                            category_undeletes = category_undeletes + @new_category_undeletes,

                            blog_article_talk_creates   = blog_article_talk_creates   + @new_blog_article_talk_creates,
                            blog_article_talk_edits     = blog_article_talk_edits     + @new_blog_article_talk_edits,
                            blog_article_talk_deletes   = blog_article_talk_deletes   + @new_blog_article_talk_deletes,
                            blog_article_talk_undeletes = blog_article_talk_undeletes + @new_blog_article_talk_undeletes,

                            project_creates   = project_creates   + @new_project_creates,
                            project_edits     = project_edits     + @new_project_edits,
                            project_deletes   = project_deletes   + @new_project_deletes,
                            project_undeletes = project_undeletes + @new_project_undeletes,

                            mediawiki_main_creates   = mediawiki_main_creates   + @new_mediawiki_main_creates,
                            mediawiki_main_edits     = mediawiki_main_edits     + @new_mediawiki_main_edits,
                            mediawiki_main_deletes   = mediawiki_main_deletes   + @new_mediawiki_main_deletes,
                            mediawiki_main_undeletes = mediawiki_main_undeletes + @new_mediawiki_main_undeletes
