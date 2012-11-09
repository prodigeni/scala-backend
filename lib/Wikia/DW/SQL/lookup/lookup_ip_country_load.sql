   LOAD DATA LOCAL INFILE 'GeoIPCity.csv'
   INTO TABLE lookup_ip_country
 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES
        (@ip_from, @ip_to, country_code, region, city, postal_code, latitude, longitude, dma_code, area_code)
    SET ip_poly = GEOMFROMWKB(POLYGON(LINESTRING( POINT(INET_ATON(@ip_from), -1),
                                                  POINT(INET_ATON(@ip_to),   -1),
                                                  POINT(INET_ATON(@ip_to),    1),
                                                  POINT(INET_ATON(@ip_from),  1),
                                                  POINT(INET_ATON(@ip_from), -1) ))),
        ip_from = INET_ATON(@ip_from),
        ip_to   = INET_ATON(@ip_to);

