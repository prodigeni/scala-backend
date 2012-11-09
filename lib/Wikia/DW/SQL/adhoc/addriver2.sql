SELECT c.country_name,
       sub.start,
       sub.stop,
       sub.stop_over_start
  FROM (
        SELECT ip.country_code,
               COUNT(CASE WHEN f.position = 'start' THEN 1 ELSE null END) AS start,
               COUNT(CASE WHEN f.position = 'stop'  THEN 1 ELSE null END) AS stop,
               ROUND(COUNT(CASE WHEN f.position = 'stop'  THEN 1 ELSE null END) /
                     COUNT(CASE WHEN f.position = 'start' THEN 1 ELSE null END), 2) AS stop_over_start
          FROM statsdb_tmp.fact_addriver f
          STRAIGHT_JOIN statsdb.lookup_ip_country ip
            ON MBRCONTAINS(ip.ip_poly, POINTFROMWKB(POINT(f.client_ip,0)))
         GROUP BY ip.country_code
       ) sub
  STRAIGHT_JOIN statsdb.lookup_country_codes c
    ON c.country_code = sub.country_code
 ORDER BY sub.start desc

