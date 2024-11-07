-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Trailheads: Student Success
-- MAGIC Student ID-level table with flags for whether or not a student graduated, dropped, etc. with event dates.

-- COMMAND ----------

-- DBTITLE 1,Unique IDs and PIDMs
CREATE OR REPLACE TEMPORARY VIEW vw_students
AS
SELECT DISTINCT
  int(student_pidm) AS student_pidm, 
  int(student_id) AS student_id, 
  to_date(date_trunc("month", faa_date)) AS faa_date,
  college_code AS college_code_faa, 
  level_code AS level_code_faa,
  program_code AS program_code_faa
FROM wgu_analytics.academics.rst_student
WHERE level_code = 'UG'
  AND faa_date >= '2023-07-01'
  AND new_start = 1
-- filter 4 duplicate student IDs
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY student_pidm, student_id
  ORDER BY faa_date
) = 1;

SELECT
  COUNT(student_id) AS ids
, COUNT(distinct student_id) as u_ids
, COUNT(student_pidm) AS pidm
, COUNT(distinct student_pidm) AS u_pidm
FROM vw_students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Graduates

-- COMMAND ----------

create or replace temporary view vw_grad
as
with grads as (
  select
    s.*
  , rst.college_code AS college_code_grad
  , rst.program_code AS program_code_grad
  , int(rst.grad) as grad_flag
  , month_end_date as grad_date
  from vw_students s
  join wgu_analytics.academics.rst_student rst
    on s.student_id = rst.student_id
    and s.student_pidm = rst.student_pidm
    and s.faa_date < rst.month_end_date
  where rst.grad = 1
  
  -- choose earliest grad record if multiple
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.student_pidm, s.student_id
    ORDER BY rst.month_end_date asc
  ) = 1
)

select
  s.*
, ifnull(g.grad_flag, 0) as grad_flag
, g.grad_date
from vw_students s
left join grads g
  on s.student_id = g.student_id
  and s.student_pidm = g.student_pidm;

select * from vw_grad 
where grad_flag = 1 limit 1000

-- COMMAND ----------

-- DBTITLE 1,Check
SELECT 
  COUNT(student_id) AS ids
, COUNT(distinct student_id) as u_ids
, COUNT(student_pidm) AS pidm
, COUNT(distinct student_pidm) AS u_pidm
FROM vw_grad

-- COMMAND ----------

-- DBTITLE 1,Cohort Grad
select
  faa_date
, count(student_id) as starts
, sum(grad_flag) as grads
, round(sum(grad_flag) / count(student_id), 3) as rate
, min(grad_date) as earliest_grad_date
, max(grad_date) as latest_grad_date
from vw_grad
where level_code_faa = 'UG'
group by 1 
order by 1

-- COMMAND ----------

select
  to_date(last_day(faa_date)) as faa_date
, int(sum(new_start)) as new_start
, int(sum(grad)) as grad
, round(sum(grad) / sum(new_start), 3) as cohort_rate
from wgu_analytics.academics.rst_student 
where faa_date >= '2023-07-01'
  and level_code = 'UG'
group by 1
order by 1 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drop

-- COMMAND ----------

create or replace temporary view vw_drop
as
with drops as (
  select
    s.*
  , 1 as drop_flag
  , d.month_end_date as drop_date
  , int(d.term_sequence) as term_sequence
  , d.drop_type
  , d.drop_sub_type
  , if(d.term_sequence = 1, 1, NULL) as drop_t1_flag
  , if(d.term_sequence = 2, 1, NULL) as drop_t2_flag
  from vw_students s
  join wgu_analytics.academics.rst_student_drops d
    on s.student_id = d.student_id
    and s.student_pidm = d.student_pidm
    and s.faa_date < d.month_end_date -- drop must occur after FAA date
    
  -- choose first drop record if multiple
  qualify row_number() over (
    partition by s.student_pidm, s.student_id
    order by month_end_date asc
  ) = 1
)

select
  s.*
, ifnull(d.drop_flag, 0) as drop_flag
, d.drop_date
, d.drop_type
, d.drop_sub_type
, ifnull(drop_t1_flag, 0) as drop_t1_flag
, ifnull(drop_t2_flag, 0) as drop_t2_flag
from vw_grad s
left join drops d
  on s.student_id = d.student_id
  and s.student_pidm = d.student_pidm
;

select * from vw_drop limit 1000

-- COMMAND ----------

-- DBTITLE 1,Check duplicate IDs
SELECT 
  COUNT(student_id) AS ids
, COUNT(distinct student_id) as u_ids
, COUNT(student_pidm) AS pidm
, COUNT(distinct student_pidm) AS u_pidm
FROM vw_drop

-- COMMAND ----------

-- DBTITLE 1,Cohort Drop
select
  faa_date
, count(student_id) as starts
, sum(drop_flag) as drops
, sum(drop_t1_flag) as t1_drops
, sum(drop_flag) / count(student_id) as rate
, sum(drop_t1_flag) / count(student_id) as t1_rate
from vw_drop
group by 1 
order by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Retention

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_retention_7m AS
select
  s.*
, rst.college_code AS college_code_r7
, rst.program_code AS program_code_r7
, INT(rst.retention_7months) AS retention_7months
, month_end_date as r7_date
from vw_students s
join wgu_analytics.academics.rst_student rst
  on s.student_id = rst.student_id
  and s.student_pidm = rst.student_pidm
  and s.faa_date < rst.month_end_date
where rst.retention_7months is not null
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.student_pidm, s.student_id
  ORDER BY rst.month_end_date
) = 1;

CREATE OR REPLACE TEMPORARY VIEW vw_retention_13m AS
select
  s.*
, rst.college_code AS college_code_r13
, rst.program_code AS program_code_r13
, INT(rst.retention_13months) AS retention_13months
, rst.month_end_date as r13_date
from vw_students s
join wgu_analytics.academics.rst_student rst
  on s.student_id = rst.student_id
  and s.student_pidm = rst.student_pidm
  and s.faa_date < rst.month_end_date
where rst.retention_13months is not null
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY s.student_pidm, s.student_id
  ORDER BY rst.month_end_date
) = 1;

CREATE OR REPLACE TEMPORARY VIEW vw_retention
AS 
SELECT
  s.*
, r7.retention_7months
, r13.retention_13months
, r7.r7_date
, r13.r13_date
FROM vw_drop s
LEFT JOIN vw_retention_7m r7 USING (student_id, student_pidm)
LEFT JOIN vw_retention_13m r13 USING (student_id, student_pidm)
;

SELECT * FROM vw_retention LIMIT 1000

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT 
  COUNT(student_id) AS ids
, COUNT(distinct student_id) as u_ids
, COUNT(student_pidm) AS pidm
, COUNT(distinct student_pidm) AS u_pidm
FROM vw_retention

-- COMMAND ----------

-- DBTITLE 1,7 and 13-month retention
WITH r7 AS (
  SELECT
    r7_date
  , ROUND(AVG(retention_7months), 2) AS r7
  , COUNT(student_id) AS n7
  FROM vw_retention
  GROUP BY 1
),

r13 AS (
  SELECT
  r13_date
  , ROUND(AVG(retention_13months), 2) AS r13
  , COUNT(student_id) AS n13
  FROM vw_retention
  GROUP BY 1
)

SELECT
  r7_date AS r_month
, r7, r13, n7, n13
FROM r7
LEFT JOIN r13 ON r7.r7_date = r13.r13_date
ORDER BY 1 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CUs

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_cu
AS
WITH competency_units AS (
  SELECT 
    s.student_id
  , s.student_pidm
  , s.faa_date
  , MAX(CASE WHEN s.term_sequence = 1 THEN limited_progress END) AS limit_prog_t1
  , MAX(CASE WHEN s.term_sequence = 2 THEN limited_progress END) AS limit_prog_t2
  , MAX(CASE WHEN s.term_sequence = 1 THEN earned_cus END) AS earned_cus_t1
  , MAX(CASE WHEN s.term_sequence = 2 THEN earned_cus END) AS earned_cus_t2
  FROM wgu_analytics.academics.rst_student s
  JOIN wgu_lakehouse.academics.inf_student_term_limited_progress lp USING (student_id)
  WHERE s.term_sequence IN (1, 2)
    AND s.faa_date >= '2023-07-01'
  group by all
  qualify row_number() OVER (
    PARTITION BY s.student_pidm, s.student_id
    ORDER BY s.faa_date
  ) = 1
)

SELECT
  s.*
, c.limit_prog_t1
, c.limit_prog_t2
, int(c.earned_cus_t1) as earned_cus_t1
, int(c.earned_cus_t2) as earned_cus_t2
FROM vw_retention s
LEFT JOIN competency_units c USING (student_id, student_pidm);

select * from vw_cu limit 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OTP

-- COMMAND ----------

-- DBTITLE 1,OTP
create or replace temporary view vw_otp 
as
with otp as (
  select 
    student_pidm
  , student_id
  , faa_date
  , int(max(case when term_sequence = 1 then term_otp end)) as t1_otp
  , int(max(case when term_sequence = 2 then term_otp end)) as t2_otp
  from wgu_analytics.academics.rst_student
  where faa_date >= '2023-07-01'
    and faa_date < month_end_date
  group by all
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY student_pidm, student_id
    ORDER BY faa_date
  ) = 1
)

SELECT
  s.*
, o.t1_otp
, o.t2_otp
FROM vw_cu s
LEFT JOIN otp o USING (student_id, student_pidm);

SELECT * FROM vw_otp LIMIT 1000;

-- COMMAND ----------

-- DBTITLE 1,Check
SELECT
  COUNT(student_id) AS stu_id
, COUNT(DISTINCT student_id) AS u_stu_id
, COUNT(student_pidm) AS pidm
, COUNT(DISTINCT student_pidm) AS u_pidm
FROM vw_otp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Export

-- COMMAND ----------

-- DBTITLE 1,Write Table
-- MAGIC %python
-- MAGIC df = spark.table("vw_otp")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("mergeSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_degree_success")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation

-- COMMAND ----------

describe users.jonathan_huck.inf_uex_degree_success

-- COMMAND ----------

select * 
from users.jonathan_huck.inf_uex_degree_success

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Notes

-- COMMAND ----------

create or replace temp view vw_cu 
as
select
  cast(term_end_date as date) term_end_date
----   month_end_date
, student_id
, sum(course_completion) course_completion
, sum(course_assign) course_assign
, sum(case when course_completion = 1 then credit_hrs else 0 end) as cus_completed_term
, sum(case when course_assign = 1 then credit_hrs else 0 end) as cus_attempted_term
, sum(sum(case when course_completion = 1 then credit_hrs else 0 end)) over (
    partition by student_id order by cast(term_end_date as date)) as cumulative_cus_earned
, sum(sum(case when course_assign = 1 then credit_hrs else 0 end)) over (
    partition by student_id order by cast(term_end_date as date)) as cumulative_cus_attempted
FROM wgu_analytics.academics.rst_assessment
WHERE to_date(term_end_date) < current_date()
  and course_number NOT IN('ORA1', 'ORA4') -- exclude orientaiton
  and (course_completion = 1 or course_assign = 1) 
group by
----   month_end_date
  cast(term_end_date as date)
 ,student_id
;

select * from vw_cu limit 100

-- COMMAND ----------

WITH cte_otp AS (
  select 
    student_pidm
  , student_id
  , int(min(case when term_sequence = 1 then term_otp end)) as term1_otp
  , int(min(case when term_sequence = 2 then term_otp end)) as term2_otp
  , to_date(min(case when term_sequence = 1 then term_end_date end)) as term1_end_date
  , to_date(min(case when term_sequence = 2 then term_end_date end)) as term2_end_date
  from wgu_analytics.academics.rst_student
  where faa_date >= '2023-07-01'
  group by all
),

cte_cus AS (
  SELECT 
    s.student_pidm
  , s.student_id
  , MAX(CASE WHEN s.term_sequence = 1 THEN LIMITED_PROGRESS END) AS t1_limit_prog
  , MAX(CASE WHEN s.term_sequence = 2 THEN LIMITED_PROGRESS END) AS t2_limit_prog
  , MAX(CASE WHEN s.term_sequence = 1 THEN earned_cus END) AS t1_earned_cus
  , MAX(CASE WHEN s.term_sequence = 2 THEN earned_cus END) AS t2_earned_cus
  FROM wgu_analytics.academics.rst_student s
  JOIN wgu_lakehouse.academics.inf_student_term_limited_progress lp USING (student_id)
  WHERE s.term_sequence IN (1, 2)
    AND s.faa_date >= '2023-07-01'
  group by all
  qualify row_number() OVER (
    PARTITION BY s.student_pidm
    ORDER BY s.student_id 
  ) = 1
),

cte_retention as (
  select distinct
    student_pidm
  , student_id
  , MAX(case when retention_7months_cohort is not null then retention_7months end)  AS retention_7months
  , MAX(case when retention_13months_cohort is not null then retention_13months end) AS retention_13months
  , MAX(case when retention_19months_cohort is not null then retention_19months end) AS retention_19months
  , MAX(case when retention_24months_cohort is not null then retention_24months end) AS retention_24months
  from wgu_analytics.academics.rst_student 
  group by all
),

cte_day45 AS (
  SELECT 
    opportunityid AS opportunity_id
  , MAX(attrition_45day_flag) AS day45
  FROM wgu_lakehouse.academics.fact_stu_45day_attrition
  WHERE wk_of_term <= 8
  GROUP BY 1
)

SELECT 
  r.*
, INT(d.day45) AS day45_attrition
, INT(b.retention_7months)  AS retention7
, INT(b.retention_13months) AS retention13
, INT(b.retention_19months) AS retention19
, INT(b.retention_24months) AS retention24
, INT(o.term1_otp) AS t1_otp
, INT(o.term2_otp) AS t2_otp
-- inverse to represent achieved limited progress at least
, CASE WHEN c.t1_limit_prog = 0 THEN 1 ELSE 0 END AS t1_limit_prog
, CASE WHEN c.t2_limit_prog = 0 THEN 1 ELSE 0 END AS t2_limit_prog
, INT(c.t1_earned_cus) AS t1_earned_cus
, INT(c.t2_earned_cus) AS t2_earned_cus
FROM vw_recommend r
LEFT JOIN cte_retention b USING (student_pidm)
LEFT JOIN cte_day45 d USING (opportunity_id)
LEFT JOIN cte_cus c USING (student_pidm)
LEFT JOIN cte_otp o USING (student_pidm)

-- COMMAND ----------


success AS (
  SELECT 
    s.*
  , case when s.faa_date != ns.new_start_date then 1 else 0 end as close_faa_mismatch
  FROM vw_new_start ns
  JOIN users.jonathan_huck.inf_trailheads_cohort_success s
    ON ns.student_pidm = s.student_pidm
    AND to_date(s.faa_date) >= to_date(ns.new_start_date)
  QUALIFY row_number() OVER (
    PARTITION BY ns.student_pidm
    ORDER BY faa_date ASC
  ) = 1  
)

