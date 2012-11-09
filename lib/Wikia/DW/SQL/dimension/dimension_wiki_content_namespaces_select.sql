SELECT w.wiki_id,
       IFNULL(variable_value, 'a:1:{i:0;i:0;}') AS namespace_id
  FROM dimension_wikis w
  LEFT JOIN dimension_wiki_variables wv
    ON wv.wiki_id = w.wiki_id
   AND wv.variable_name = 'wgContentNamespaces'

