/* 
 *
 * Schema and queries to support initial set of reports defined:
 * https://internal.wikia-inc.com/index.php?title=Reports_Library&&cb=3526
 *
 * Author: Reed Sandberg
 * Date: Oct 14, 2011
 *
 * Notes:
 * Designed for partitioning and partition management by Oracle RDBMS - mysql is questionable?
 * Uses Oracle analytic functions, which must be re-written if using mysql.
 *
 */

/* Dimension tables */
CREATE TABLE wiki
(
    wiki_id
    name
    create_date
    primary_cat /* Primary category */
)
/

CREATE TABLE user
(
    uber_id
    first_edit_date
    first_chat_date
)
/

/* Aggregated fact tables, assume partition management where date partitions are aged out periodically */
CREATE TABLE user_activity_day_agg
(
    uber_id
    wiki_id
    user_type
    period_date
    edit_create_page_cnt
    edit_delete_cnt
    edit_edit_cnt
    edit_undelete_cnt
    view_cnt
    view_bounce_cnt
    fb_connect_cnt ?
    chat_cnt
    wiki_create_cnt
)
PARITION BY RANGE(period_date) INTERVAL (NUMTODSINTERVAL(1,'DAY'))
SUBPARTITION BY LIST(user_type)
SUBPARTITION TEMPLATE
   ( SUBPARTITION p_bot VALUES ('BOT')
   , SUBPARTITION p_editor VALUES ('EDITOR')
   , SUBPARTITION p_staff VALUES ('STAFF'))
( PARTITION before_2000 VALUES LESS THAN (TO_DATE('01-OCT-2010','DD-MON-YYYY')))
/

CREATE TABLE user_activity_week_agg
(
    uber_id
    wiki_id
    user_type
    period_date
    edit_create_page_cnt
    edit_delete_cnt
    edit_edit_cnt
    edit_undelete_cnt
    view_cnt
    view_bounce_cnt
    fb_connect_cnt ?
    chat_cnt
    wiki_create_cnt
)
PARITION BY RANGE(period_date) INTERVAL (NUMTODSINTERVAL(7,'DAY'))
SUBPARTITION BY LIST(user_type)
SUBPARTITION TEMPLATE
   ( SUBPARTITION p_bot VALUES ('BOT')
   , SUBPARTITION p_editor VALUES ('EDITOR')
   , SUBPARTITION p_staff VALUES ('STAFF'))
( PARTITION before_2000 VALUES LESS THAN (TO_DATE('01-OCT-2010','DD-MON-YYYY')))
/

CREATE TABLE user_activity_month_agg
(
    uber_id
    wiki_id
    user_type
    period_date
    edit_create_page_cnt
    edit_delete_cnt
    edit_edit_cnt
    edit_undelete_cnt
    view_cnt
    view_bounce_cnt
    fb_connect_cnt ?
    chat_cnt
    wiki_create_cnt
)
PARITION BY RANGE(period_date) INTERVAL (NUMTODSINTERVAL(7,'DAY'))
SUBPARTITION BY LIST(user_type)
SUBPARTITION TEMPLATE
   ( SUBPARTITION p_bot VALUES ('BOT')
   , SUBPARTITION p_editor VALUES ('EDITOR')
   , SUBPARTITION p_staff VALUES ('STAFF'))
( PARTITION before_2000 VALUES LESS THAN (TO_DATE('01-OCT-2010','DD-MON-YYYY')))
/


/* Number of Edits per week */
SELECT TO_CHAR(period_date,'YYYYMMDD') first_day_of_week,
  SUM(edit_create_page_cnt) page_create_cnt,
  SUM(edit_delete_cnt) delete_cnt,
  SUM(edit_edit_cnt) edit_cnt,
  SUM(edit_undelete_cnt)
FROM user_activity_week_agg
WHERE
 <DATE_RANGE>
 <NAMESPACE_FILTER>
 AND user_type = 'EDITOR'
GROUP BY period_date
/


/* Number of Editors making X edits per week */
SELECT TO_CHAR(period_date,'YYYYMMDD') first_day_of_week,
  CASE WHEN all_edits_cnt BETWEEN 0 AND 5 THEN '0-5'
  WHEN all_edits_cnt BETWEEN 6 AND 10 THEN '6-50'
  WHEN all_edits_cnt BETWEEN 51 AND 100 THEN '61-100'
  WHEN all_edits_cnt BETWEEN 101 AND 500 THEN '101-500' END edit_cnt_bin,
  COUNT(*) wiki_editor_cnt
FROM
  (SELECT period_date, uber_id, SUM(edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt) all_edits_cnt
  FROM user_activity_week_agg
  WHERE
   <DATE_RANGE>
   AND user_type = 'EDITOR'
  GROUP BY period_date, uber_id
  ) edit_week_cnt
GROUP BY period_date,
  CASE WHEN all_edits_cnt BETWEEN 0 AND 5 THEN '0-5'
  WHEN all_edits_cnt BETWEEN 6 AND 10 THEN '6-50'
  WHEN all_edits_cnt BETWEEN 51 AND 100 THEN '61-100'
  WHEN all_edits_cnt BETWEEN 101 AND 500 THEN '101-500' END
/


/* Number of Editors editing the same wiki for X number of days per month */
SELECT month_of_year, edit_days_mon, COUNT(*) wiki_editor_cnt
FROM
  (SELECT TO_CHAR(period_date,'YYYYMM') month_of_year, wiki_id, uber_id, COUNT(DISTINCT TO_CHAR(period_date,'YYYYMMDD')) edit_days_mon
  FROM user_activity_day_agg
  WHERE
   <DATE_RANGE>
   AND user_type = 'EDITOR'
   AND edit_edit_cnt + edit_create_page_cnt > 0
  GROUP BY TO_CHAR(period_date,'YYYYMM'), wiki_id, uber_id
  ) edit_day_cnt
GROUP BY month_of_year, edit_days_mon
/


/* Percentage of Users viewing X number of weeks per week */
SELECT TO_CHAR(period_date,'YYYYMMDD') first_day_of_week, wikis_viewed_user, COUNT(*) viewer_cnt
FROM
  (SELECT period_date, uber_id, COUNT(DISTINCT wiki_id) wikis_viewed_user
  FROM user_activity_week_agg
  WHERE
   <DATE_RANGE>
   AND user_type = 'BROWSER'
   AND view_cnt > 0
  GROUP BY period_date, uber_id
  ) wiki_view_cnt
GROUP BY period_date, wikis_viewed_user
/


/* Pageview GA overlaid with edits */
SELECT TO_CHAR(period_date,'YYYYMMDD') AS "Day", SUM(edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt) all_edits_cnt, SUM(view_cnt) ga_view_cnt
FROM user_activity_day_agg
WHERE
 <DATE_RANGE>
GROUP BY period_date
/


/* Pageview GA and Bounce Rate GA trends */
SELECT TO_CHAR(period_date,'YYYYMMDD') AS "Day", SUM(view_bounce_cnt) view_bounce_cnt, SUM(view_cnt) ga_view_cnt
FROM user_activity_day_agg
WHERE
 <DATE_RANGE>
GROUP BY period_date
/


/* Editors Multiple Wikis */
SELECT
  CASE WHEN wikis_edited_user = 1 THEN '1'
  WHEN wikis_edited_user = 2 THEN '2'
  WHEN wikis_edited_user = 3 THEN '3'
  WHEN wikis_edited_user = 4 THEN '4'
  WHEN wikis_edited_user BETWEEN 5 AND 10 THEN '5-10'
  WHEN wikis_edited_user BETWEEN 11 AND 50 THEN '11-50' END wikis_edited_cnt_bin,
  COUNT(*) editor_cnt
FROM
  (SELECT uber_id, COUNT(DISTINCT wiki_id) wikis_edited_user
  FROM user_activity_month_agg
  WHERE
   <DATE_RANGE>
   AND user_type = 'EDITOR'
   AND edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt > 0
  GROUP BY uber_id
  ) wiki_edited_cnt
GROUP BY
  CASE WHEN wikis_edited_user = 1 THEN '1'
  WHEN wikis_edited_user = 2 THEN '2'
  WHEN wikis_edited_user = 3 THEN '3'
  WHEN wikis_edited_user = 4 THEN '4'
  WHEN wikis_edited_user BETWEEN 5 AND 10 THEN '5-10'
  WHEN wikis_edited_user BETWEEN 11 AND 50 THEN '11-50' END
/


/* Wikis with Edits */
SELECT TO_CHAR(period_date,'YYYYMMDD') first_day_of_week, COUNT(DISTINCT wiki_id) wiki_edited_cnt
FROM user_activity_week_agg
WHERE
 <DATE_RANGE>
 AND user_type = 'EDITOR'
 AND edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt > 0
GROUP BY period_date
/


/* FB Connect - need to understand acquisition of facebook data and how/if we can match based on user */


/* Pageview GA Heatmap */
SELECT w.name, TO_CHAR(w.create_date,'YYYYMMDD') wiki_create_date, TO_CHAR(period_date,'YYYYMMDD') pageview_date,
  ROUND(100 * RATIO_TO_REPORT (SUM(view_cnt)) OVER (PARTITION BY agg.wiki_id), 2) pageview_pct_day
FROM user_activity_day_agg agg, wiki w
WHERE
 <DATE_RANGE>
 AND agg.wiki_id IN (...)
 AND w.wiki_id = agg.wiki_id
GROUP BY w.name, TO_CHAR(w.create_date,'YYYYMMDD'), period_date
ORDER BY w.name
/


/* Pageview GA Top Wikis (include as percentage of total) */
SELECT w.wiki_id, w.name, TO_CHAR(period_date,'YYYYMMDD') AS "Day", SUM(view_cnt) views, ROUND(100 * RATIO_TO_REPORT (SUM(view_cnt)) OVER (PARTITION BY w.wiki_id), 2) views_pct_total
FROM user_activity_day_agg agg, wiki w
WHERE
 <DATE_RANGE>
 AND w.wiki_id = agg.wiki_id
GROUP BY w.wiki_id, w.name, period_date
HAVING SUM(view_cnt) >= 10
ORDER BY views_pct_total DESC
/


/* Users using Chat */
SELECT w.name, w.primary_cat, TO_CHAR(period_date,'YYYYMMDD') chat_day, SUM(chat_cnt) chat_event_cnt
FROM user_activity_day_agg agg, wiki w
WHERE
 <DATE_RANGE>
 AND w.wiki_id = agg.wiki_id
GROUP BY w.name, w.primary_cat, period_date
HAVING SUM(chat_cnt) > 0
ORDER BY w.name
/


/* Users using chat Total by day */
SELECT TO_CHAR(period_date,'YYYYMMDD') chat_day, COUNT(DISTINCT uber_id) chatter_cnt
FROM user_activity_day_agg
WHERE
 <DATE_RANGE>
 AND chat_cnt > 0
GROUP BY period_date
ORDER BY period_date
/


/* Cohort Percentage Edits versus Age */
SELECT TO_CHAR(c.create_date,'YYYYMMDD') create_date, age_days, cohorts_edited, ROUND(cohorts_edited / cohorts_created * 100, 2) cohorts_edited_pct
FROM
  (SELECT w.create_date, period_date - w.create_date age_days, COUNT(DISTINCT w.wiki_id) cohorts_edited
  FROM user_activity_day_agg agg, wiki w
  WHERE
   <DATE_RANGE (on w.create_date)>
   AND agg.wiki_id = w.wiki_id
   AND period_date - w.create_date BETWEEN <AGE_RANGE> /* rewrite as period_date BETWEEN to use index */
   AND edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt > 0
  GROUP BY w.create_date, period_date - w.create_date) e,
  (SELECT create_date, COUNT(*) cohorts_created
  FROM wiki
  WHERE <DATE_RANGE (on w.create_date)>
  GROUP BY create_date) c
WHERE c.create_date = e.create_date
/


/* Retention of Editors */
SELECT first_edit_date, last_edit_date - first_edit_date editor_retention_days, COUNT(DISTINCT uber_id) cohorts_editors
FROM
  (SELECT u.uber_id, u.first_edit_date, MAX(period_date) last_edit_date
  FROM user_activity_day_agg agg, user u
  WHERE
   <DATE_RANGE on u.first_edit_date>
   AND agg.uber_id = u.uber_id
   AND edit_create_page_cnt+edit_delete_cnt+edit_edit_cnt+edit_undelete_cnt > 0
  GROUP BY u.uber_id, u.first_edit_date) l
GROUP BY first_edit_date, last_edit_date - first_edit_date
/


/* Retention of Chat Users */
SELECT first_chat_date, last_chat_date - first_chat_date chatter_retention_days, COUNT(DISTINCT uber_id) cohorts_chatters
FROM
  (SELECT u.uber_id, u.first_chat_date, MAX(period_date) last_chat_date
  FROM user_activity_day_agg agg, user u
  WHERE
   <DATE_RANGE on u.first_chat_date>
   AND agg.uber_id = u.uber_id
   AND chat_cnt > 0
  GROUP BY u.uber_id, u.first_chat_date) l
GROUP BY first_chat_date, last_chat_date - first_chat_date
/

