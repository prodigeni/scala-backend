SELECT '[time_id]' AS time_id,
       wiki_id,
       COUNT(DISTINCT article_id) AS articles,
       COUNT(1) AS total_embeds,
       COUNT(CASE WHEN premium = 1 THEN 1 ELSE null END) AS premium_embeds
  FROM dimension_wiki_embeds
 GROUP BY wiki_id
