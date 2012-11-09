SELECT pv.country_code AS "Country Code",
       c.country_name  AS "Country Name",
       sub.pageviews   AS "Pageviews (no beacon)",
       pv.pageviews    AS "Pageviews 07/11)",
       ROUND(100 * sub.pageviews / pv.pageviews, 2) AS "% of Pageviews (no beacon)",
       sub.percent_pageviews AS "% of No-Beacon Pageviews"
  FROM (
        SELECT ip.country_code,
               COUNT(1) AS pageviews
          FROM fact_pageview_events e
          JOIN statsdb.lookup_ip_country ip
            ON MBRCONTAINS(ip_poly, POINTFROMWKB(POINT(e.ip,0)))
         WHERE e.event_ts >= '2012-07-11'
           AND e.event_ts <  '2012-07-12'
         GROUP BY ip.country_code
       ) pv
  JOIN statsdb.lookup_country_codes c
    ON c.country_code = pv.country_code
  JOIN (
        SELECT ip.country_code,
               SUM(cnt) AS pageviews,
               ROUND(100 * SUM(cnt) / (SELECT SUM(cnt) FROM statsdb_tmp.no_beacon_ips), 2) AS percent_pageviews
          FROM statsdb_tmp.no_beacon_ips e
          JOIN statsdb.lookup_ip_country ip
            ON MBRCONTAINS(ip_poly, POINTFROMWKB(POINT(e.ip,0)))
         GROUP BY ip.country_code
       ) sub
    ON sub.country_code = pv.country_code
 ORDER BY sub.pageviews DESC



