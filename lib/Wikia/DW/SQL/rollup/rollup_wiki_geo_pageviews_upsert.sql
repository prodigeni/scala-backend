INSERT INTO rollup_wiki_geo_pageviews (
    period_id,
    time_id,
    wiki_id,
    country_code,
    region,
    city,
    pageviews
)
SELECT [period_id] AS period_id,
       [time_id]   AS time_id,
       e.wiki_id,
       ip.country_code,
       ip.region,
       ip.city,
       @new_pageviews := COUNT(1) AS new_pageviews
  FROM statsdb.fact_pageview_events e
  JOIN statsdb.lookup_ip_country ip
    ON MBRCONTAINS(ip_poly, POINTFROMWKB(POINT(e.ip,0)))
 WHERE e.event_ts BETWEEN TIMESTAMP('[begin_time]')
                      AND TIMESTAMP('[end_time]')
   AND e.file_id = [file_id]
   AND e.ip IS NOT NULL
 GROUP BY period_id,
          time_id,
          e.wiki_id,
          ip.country_code,
          ip.region,
          ip.city
    ON DUPLICATE KEY UPDATE pageviews = pageviews + @new_pageviews
