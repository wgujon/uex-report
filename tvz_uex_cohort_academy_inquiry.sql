-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Recommendation Received Cohort

-- COMMAND ----------

-- Start with the Academy inquiry table
SELECT 
  COUNT(opportunity_id) AS ids
, COUNT(DISTINCT opportunity_id) AS unique_ids
FROM wgubidev.inf_uex_academy_inquiry

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recommendations

-- COMMAND ----------

-- DBTITLE 1,Recommendations
CREATE OR REPLACE TEMPORARY VIEW vw_recommend 
AS
SELECT
  a.*
, CASE 
    WHEN acad_program_of_interest = 'Certificates' THEN NULL
    ELSE acad_program_of_interest END AS acad_prog_interest_without_cert
, ifnull(r.recommend_flag, 0) AS recommend_flag
, r.recommend_date
, r.recommend_datetime
, r.recommend_product_category
, r.recommend_product_type
, r.recommend_reason
, r.recommend_products
, coalesce(r.recommend_stage, 'Missing') as recommend_stage
, r.care_status_at_recommend
, ifnull(r.not_admitted_at_rec, 0) AS not_admitted_at_rec
, r.latest_care_status
, CASE r.college_code
    WHEN 'BU' THEN 'WSB'
    WHEN 'HE' THEN 'LSH'
    WHEN 'TC' THEN 'WSE'
    WHEN 'IT' THEN 'WST'
    END AS college_code_rec
, r.level_code AS level_code_rec
, r.program_code AS program_code_rec
FROM wgubidev.inf_uex_academy_inquiry a
LEFT JOIN users.jonathan_huck.inf_uex_recommend r USING (opportunity_id);

SELECT * FROM vw_recommend LIMIT 100

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT 
  COUNT(opportunity_id) AS ids
, COUNT(DISTINCT opportunity_id) AS unique_ids
FROM vw_recommend

-- COMMAND ----------

 SELECT 
    ifnull(coalesce(college_code_rec, acad_prog_interest_without_cert), 'Missing') AS school_code
  , COUNT(opportunity_id) as n
  , COUNT(distinct opportunity_id) as unique_n
  FROM vw_recommend
  GROUP BY 1


-- COMMAND ----------

-- DBTITLE 1,Check School Code
WITH academy_code AS (
  SELECT 
    CASE 
      WHEN acad_program_of_interest = 'Certificates' THEN 'Missing'
      WHEN acad_program_of_interest IS NULL THEN 'Missing'
      ELSE acad_program_of_interest END AS school_code
  , COUNT(opportunity_id) as n
  , COUNT(distinct opportunity_id) as unique_n
  FROM vw_recommend
  GROUP BY 1
),

wgu_code AS (
  SELECT 
    ifnull(coalesce(college_code_rec, acad_prog_interest_without_cert), 'Missing') AS school_code
  , COUNT(opportunity_id) as n
  , COUNT(distinct opportunity_id) as unique_n
  FROM vw_recommend
  GROUP BY 1
),

recommend AS (
  SELECT 
    CASE college_code
      WHEN 'BU' THEN 'WSB'
      WHEN 'HE' THEN 'LSH'
      WHEN 'TC' THEN 'WSE'
      WHEN 'IT' THEN 'WST'
      ELSE 'Missing'
      END AS school_code
  , COUNT(opportunity_id) as n
  , COUNT(distinct opportunity_id) as unique_n
  FROM users.jonathan_huck.inf_uex_recommend
  WHERE recommend_date >= '2024-01-22'
  GROUP BY 1
)

SELECT 
  a.school_code
, a.n as academy_code
, w.n as wgu_code
, round((w.n - a.n) / a.n, 2) as prop_diff
, r.n as ir_rec
, round((w.n - r.n) / r.n, 2) as error
FROM academy_code a
FULL JOIN wgu_code w USING (school_code)
LEFT JOIN recommend r USING (school_code)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Degree Starts
-- MAGIC Skipped for now... too much difference from the counts Academy is currently using from their reports.

-- COMMAND ----------

create or replace temporary view vw_degree_start
as
with unique_degree_starts as (
  select
    a.opportunity_id
  , 1 as degree_start_flag
  , b.start_date as degree_start_date
  , b.college_code as college_code_start
  , b.level_code as level_code_start
  , b.program_code as program_code_start
  from vw_recommend a
  join users.jonathan_huck.inf_uex_degree_start b 
    on b.opportunity_id = a.opportunity_id
    and b.start_date >= a.received_date
  qualify row_number() over (
    partition by a.opportunity_id
    order by b.start_date asc
  ) = 1
)

select
  a.*
, ifnull(b.degree_start_flag, 0) as degree_start_flag
, b.degree_start_date
, datediff(b.degree_start_date, a.app_date) AS days_app_to_degree_start
--, ifnull(d.day45_flag, 0) AS day45_flag
from vw_recommend a
left join unique_degree_starts b using (opportunity_id);

select * from vw_degree_start limit 100;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT 
  COUNT(opportunity_id) AS ids
, COUNT(DISTINCT opportunity_id) AS unique_ids
FROM vw_degree_start

-- COMMAND ----------

select 
  received_month
, COUNT(*) as Referrals
, SUM(case when recommend_flag = 1 then 1 else 0 end) as recommend
, sum(started_onramp) as started_onramp
, SUM(Started_Degree) as Started_Degree_orig
, SUM(degree_start_flag) as Started_Degree_join
, SUM(Started_OnRamp_and_Degree) as Started_OnRamp_and_Degree_orig
, SUM(case when degree_start_flag = 1 and acad_start_date is not null then 1 else 0 end) As started_onramp_degree_join
, SUM(Started_Direct_Degree) as Started_Direct_Degree_orig
, SUM(case when degree_start_flag = 1 and started_onramp = 0 then 1 else 0 end) As direct_degree_join
from vw_degree_start
--where product_category != "Certificate"
--  and academy_enrollment_status != "Prospecting"
group by 1
order by 1

-- COMMAND ----------

select 
  received_month
, COUNT(*) as Referrals
, SUM(case when recommend_flag = 1 then 1 else 0 end) as recommend
, sum(started_onramp) as started_onramp
, SUM(Started_Degree) as Started_Degree
, SUM(Started_OnRamp_and_Degree) as Started_OnRamp_and_Degree
, SUM(Started_Direct_Degree) as Started_Direct_Degree
from vw_degree_start
--where product_category != "Certificate"
--  and academy_enrollment_status != "Prospecting"
group by 1
order by 1

-- COMMAND ----------

select 
  cohort_month_end
, COUNT(*) as Referrals
, sum(started_onramp) as started_onramp
, SUM(Started_WGU) as Started_Degree
, SUM(Started_OnRamp_and_WGU) as Started_OnRamp_and_Degree
--, SUM(Started_Direct_Degree) as Started_Direct_Degree
from wgubidev.tvz_trailheads_referral_cohort
--where product_category != "Certificate"
--  and academy_enrollment_status != "Prospecting"
group by 1
order by 1

-- COMMAND ----------

describe vw_degree_start

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_export
AS
SELECT 
  *
, ifnull(coalesce(college_code_rec, acad_prog_interest_without_cert), 'Missing') AS college_coalesced
FROM vw_recommend

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Export

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""select * from vw_export""")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.tvz_uex_cohort_academy_inquiry")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation

-- COMMAND ----------

describe users.jonathan_huck.tvz_uex_cohort_academy_inquiry

-- COMMAND ----------

select * from users.jonathan_huck.tvz_uex_cohort_academy_inquiry limit 100

-- COMMAND ----------

with academy as (
  select 
    last_day(acad_start_date) as report_month
  , count(opportunity_id) as n_all
  from users.jonathan_huck.tvz_uex_cohort_academy_inquiry
  where acad_start_date is not null
  group by 1
),

trailheads as (
  select 
    last_day(acad_start_date) as report_month
  , count(opportunity_id) as n_th
  from users.jonathan_huck.tvz_uex_cohort_academy_inquiry
  where acad_start_date is not null
    and client_subclass = 'Trailheads'
  group by 1
),

recommend as (
  select 
    last_day(acad_start_date) as report_month
  , count(opportunity_id) as n_rec
  from users.jonathan_huck.tvz_uex_cohort_academy_inquiry
  where acad_start_date is not null
    and recommend_flag = 1
  group by 1
)

select
  a.report_month
, a.n_all
, b.n_th
, c.n_rec
from academy a
left join trailheads b using (report_month)
left join recommend c using (report_month)
order by 1
