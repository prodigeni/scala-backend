SELECT w.domain, sub.*
  FROM (SELECT month,
               RANK() OVER (PARTITION BY month ORDER BY pageviews DESC NULLS LAST, wiki_id ASC) AS rank,
               wiki_id,
               pageviews
          FROM monthly_wiki_pageviews
         ORDER BY month, rank
     ) sub 
  JOIN dimension_wikis w
    ON w.wiki_id = sub.wiki_id
 WHERE sub.rank <= 50
 ORDER BY month,
          rank

