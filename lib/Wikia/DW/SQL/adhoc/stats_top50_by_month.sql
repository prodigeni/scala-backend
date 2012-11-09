SELECT *
  FROM (
        SELECT pv.month,
               RANK() OVER (PARTITION BY pv.month ORDER BY pv.pageviews DESC NULLS LAST, pv.wiki_id ASC) AS rank,
               w.domain,
               pv.pageviews
          FROM stats_monthly_wiki_pageviews pv
          JOIN dimension_all_wikis w
            ON w.wiki_id = pv.wiki_id
       ) sub
 WHERE sub.rank <= 50
 ORDER BY month,
          rank

