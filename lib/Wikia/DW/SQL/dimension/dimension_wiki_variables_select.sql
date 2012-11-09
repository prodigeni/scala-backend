SELECT cv.cv_city_id AS wiki_id,
       cvp.cv_name   AS variable_name,
       cv.cv_value   AS variable_value
  FROM city_variables cv
  JOIN city_variables_pool cvp
    ON cvp.cv_id = cv.cv_variable_id
 WHERE cv.cv_variable_id IN (359, 590, 615, 1053, 1198)

