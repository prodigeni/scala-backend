SELECT d.domain,
       w.wiki_id,
       w.hub_name,
       w.created_at,
       SUM(CASE WHEN e.time_id = '2008-01-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080101,
       SUM(CASE WHEN e.time_id = '2008-02-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080201,
       SUM(CASE WHEN e.time_id = '2008-03-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080301,
       SUM(CASE WHEN e.time_id = '2008-04-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080401,
       SUM(CASE WHEN e.time_id = '2008-05-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080501,
       SUM(CASE WHEN e.time_id = '2008-06-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080601,
       SUM(CASE WHEN e.time_id = '2008-07-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080701,
       SUM(CASE WHEN e.time_id = '2008-08-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080801,
       SUM(CASE WHEN e.time_id = '2008-09-01' THEN e.content_creates ELSE 0 END) AS content_creates_20080901,
       SUM(CASE WHEN e.time_id = '2008-10-01' THEN e.content_creates ELSE 0 END) AS content_creates_20081001,
       SUM(CASE WHEN e.time_id = '2008-11-01' THEN e.content_creates ELSE 0 END) AS content_creates_20081101,
       SUM(CASE WHEN e.time_id = '2008-12-01' THEN e.content_creates ELSE 0 END) AS content_creates_20081201,
       SUM(CASE WHEN e.time_id = '2009-01-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090101,
       SUM(CASE WHEN e.time_id = '2009-02-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090201,
       SUM(CASE WHEN e.time_id = '2009-03-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090301,
       SUM(CASE WHEN e.time_id = '2009-04-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090401,
       SUM(CASE WHEN e.time_id = '2009-05-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090501,
       SUM(CASE WHEN e.time_id = '2009-06-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090601,
       SUM(CASE WHEN e.time_id = '2009-07-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090701,
       SUM(CASE WHEN e.time_id = '2009-08-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090801,
       SUM(CASE WHEN e.time_id = '2009-09-01' THEN e.content_creates ELSE 0 END) AS content_creates_20090901,
       SUM(CASE WHEN e.time_id = '2009-10-01' THEN e.content_creates ELSE 0 END) AS content_creates_20091001,
       SUM(CASE WHEN e.time_id = '2009-11-01' THEN e.content_creates ELSE 0 END) AS content_creates_20091101,
       SUM(CASE WHEN e.time_id = '2009-12-01' THEN e.content_creates ELSE 0 END) AS content_creates_20091201,
       SUM(CASE WHEN e.time_id = '2010-01-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100101,
       SUM(CASE WHEN e.time_id = '2010-02-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100201,
       SUM(CASE WHEN e.time_id = '2010-03-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100301,
       SUM(CASE WHEN e.time_id = '2010-04-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100401,
       SUM(CASE WHEN e.time_id = '2010-05-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100501,
       SUM(CASE WHEN e.time_id = '2010-06-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100601,
       SUM(CASE WHEN e.time_id = '2010-07-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100701,
       SUM(CASE WHEN e.time_id = '2010-08-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100801,
       SUM(CASE WHEN e.time_id = '2010-09-01' THEN e.content_creates ELSE 0 END) AS content_creates_20100901,
       SUM(CASE WHEN e.time_id = '2010-10-01' THEN e.content_creates ELSE 0 END) AS content_creates_20101001,
       SUM(CASE WHEN e.time_id = '2010-11-01' THEN e.content_creates ELSE 0 END) AS content_creates_20101101,
       SUM(CASE WHEN e.time_id = '2010-12-01' THEN e.content_creates ELSE 0 END) AS content_creates_20101201,
       SUM(CASE WHEN e.time_id = '2011-01-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110101,
       SUM(CASE WHEN e.time_id = '2011-02-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110201,
       SUM(CASE WHEN e.time_id = '2011-03-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110301,
       SUM(CASE WHEN e.time_id = '2011-04-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110401,
       SUM(CASE WHEN e.time_id = '2011-05-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110501,
       SUM(CASE WHEN e.time_id = '2011-06-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110601,
       SUM(CASE WHEN e.time_id = '2011-07-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110701,
       SUM(CASE WHEN e.time_id = '2011-08-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110801,
       SUM(CASE WHEN e.time_id = '2011-09-01' THEN e.content_creates ELSE 0 END) AS content_creates_20110901,
       SUM(CASE WHEN e.time_id = '2011-10-01' THEN e.content_creates ELSE 0 END) AS content_creates_20111001,
       SUM(CASE WHEN e.time_id = '2011-11-01' THEN e.content_creates ELSE 0 END) AS content_creates_20111101,
       SUM(CASE WHEN e.time_id = '2011-12-01' THEN e.content_creates ELSE 0 END) AS content_creates_20111201,
       SUM(CASE WHEN e.time_id = '2012-01-01' THEN e.content_creates ELSE 0 END) AS content_creates_20120101,
       SUM(CASE WHEN e.time_id = '2012-02-01' THEN e.content_creates ELSE 0 END) AS content_creates_20120201,
       SUM(CASE WHEN e.time_id = '2012-03-01' THEN e.content_creates ELSE 0 END) AS content_creates_20120301
  FROM dimension_domains_top20k d
  LEFT JOIN dimension_wikis w
    ON w.domain = d.domain
  LEFT JOIN rollup_wiki_old_events e
    ON e.period_id = 3
   AND e.time_id >= '2008-01-01'
   AND e.time_id <  '2012-04-01'
   AND e.wiki_id = w.wiki_id
 GROUP BY w.wiki_id
 ORDER BY d.rank
