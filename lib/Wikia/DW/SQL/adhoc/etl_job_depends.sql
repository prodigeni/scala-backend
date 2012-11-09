SELECT j.job_class,
       j.job_params,
       pt.time_id,
       pt.begin_time,
       pt.end_time,
       MAX(e.executed_at) AS executed_at,
       COUNT(DISTINCT jd.depends_class, jd.depends_params) AS dependencies
  FROM etl_jobs j
  JOIN etl_period_times pt
    ON pt.period_id = 15
   AND pt.begin_time >= j.begin_time
   AND pt.end_time   <= j.end_time
   AND pt.begin_time >= DATE_SUB(now(), INTERVAL 1 DAY)
   AND pt.end_time   <= now()
  LEFT JOIN etl_job_executions e
    ON e.job_id  = j.job_id
   AND e.time_id = pt.time_id
   AND e.status = 'SUCCESS'
  LEFT JOIN etl_job_depends jd
    ON jd.job_class  = j.job_class
   AND jd.job_params = j.job_params
   AND jd.begin_time <  pt.time_id
   AND jd.end_time   >= pt.time_id
 WHERE j.queue = 'Fact'
   AND j.job_class = 'Fact'
   AND j.job_params = 'fact_pageview_events'
 GROUP BY j.job_id,
          pt.time_id
 ORDER BY j.job_class,
          j.job_params,
          pt.time_id



