SELECT f.time_id,
       c.country_name,
       COUNT(CASE WHEN f.position = 'start' THEN 1 ELSE null END) AS start,
       COUNT(CASE WHEN f.position = 'stop'  THEN 1 ELSE null END) AS stop,
       ROUND(COUNT(CASE WHEN f.position = 'stop'  THEN 1 ELSE null END) /
             COUNT(CASE WHEN f.position = 'start' THEN 1 ELSE null END), 2) AS stop_over_start
  FROM fact_addriver f
  JOIN statsdb.lookup_ip_country ip
    ON MBRCONTAINS(ip_poly, POINTFROMWKB(POINT(f.client_ip,0)))
  JOIN statsdb.lookup_country_codes c
    ON c.country_code = ip.country_code
 GROUP BY time_id,
          c.country_code
