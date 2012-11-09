SELECT user_id,
       user_name,
       user_real_name,
       user_email,
       user_email_authenticated,
       user_editcount,
       STR_TO_DATE(user_registration, '%Y%m%d%H%i%s') AS user_registration,
       CASE WHEN b.ug_user  IS NULL THEN false ELSE true END AS is_bot,
       CASE WHEN bg.ug_user IS NULL THEN false ELSE true END AS is_bot_global,
       CASE WHEN IFNULL(up.up_value,0) = 0 THEN false ELSE true END AS user_marketingallowed
  FROM user u
  LEFT JOIN user_groups b
    ON b.ug_user  = u.user_id
   AND b.ug_group = 'bot'
  LEFT JOIN user_groups bg
    ON bg.ug_user  = u.user_id
   AND bg.ug_group = 'bot-global'
  LEFT JOIN user_properties up
    ON up.up_user = u.user_id
   AND up.up_property = 'marketingallowed'

