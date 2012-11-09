SELECT city.city_id            AS wiki_id,
       city.city_dbname        AS dbname,
       city.city_sitename      AS sitename,
       REPLACE(REPLACE(city.city_url, 'http://', ''), '/', '') AS url,
       REPLACE(REPLACE(city.city_url, 'http://', ''), '/', '') AS domain,
       city.city_founding_user AS founding_user_id,
       city.city_public        AS public,
       city.city_lang          AS lang,
       lang.lang_id            AS lang_id,
       cat.cat_id              AS hub_id,
       cat.cat_name            AS hub_name,
       IFNULL(city.city_cluster, 'c1') AS cluster,
       city.city_created       AS created_at,
       0                       AS deleted
  FROM city_list city
  LEFT JOIN city_cat_mapping m
    ON m.city_id = city.city_id
  LEFT JOIN city_cats cat
    ON cat.cat_id = m.cat_id
  LEFT JOIN city_lang lang
    ON lang.lang_code = city.city_lang
