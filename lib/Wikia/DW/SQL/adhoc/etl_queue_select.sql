SELECT j.job_id,
       j.class     AS job_class,
       j.params    AS job_params,
       j.period_id AS job_period_id,
       jd.depends_class,
       jd.depends_params,
       jd.depends_period_id,
       jd.wait_for_complete,
       pt.time_id,
       ds.dependency_status
  FROM statsdb_etl.etl_jobs j
  LEFT JOIN statsdb_etl.etl_job_dependencies jd
    ON jd.job_class     = j.class
   AND jd.job_params    = j.params
   AND jd.job_period_id = j.period_id
  JOIN statsdb_etl.etl_period_times pt
    ON pt.period_id = j.period_id
   AND pt.time_id >= GREATEST( j.begin_time, jd.begin_time )
   AND pt.time_id <  LEAST(    j.end_time,   jd.end_time   )
  LEFT JOIN statsdb_etl.etl_job_dependency_status ds
    ON ds.job_class     = jd.depends_class
   AND ds.job_params    = jd.depends_params
   AND ds.job_period_id = jd.depends_period_id
 WHERE now() >= j.begin_time
   AND now() <  j.end_time
   AND j.job_id = 5
 LIMIT 40





































