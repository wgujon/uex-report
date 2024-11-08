-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Trailheads Cohort Report: Account Created

-- COMMAND ----------

SELECT 
  count(opportunity_id) AS ids
, count(distinct opportunity_id) AS unique_ids
, abs(count(opportunity_id) - count(distinct opportunity_id)) AS diff
FROM users.jonathan_huck.inf_uex_account
WHERE account_created_date < current_date()

-- COMMAND ----------

-- DBTITLE 1,Application
create or replace temporary view vw_app as
select 
  a.*
, b.app_flag
, b.app_date
, b.app_datetime
, datediff(b.app_date, a.account_created_date) AS days_account_to_app
from users.jonathan_huck.inf_uex_account a
left join users.jonathan_huck.inf_uex_app b
  on a.opportunity_id = b.opportunity_id
  and b.app_date >= a.account_created_date
;

-- COMMAND ----------

SELECT 
  count(opportunity_id) AS ids
, count(distinct opportunity_id) AS unique_ids
, abs(count(opportunity_id) - count(distinct opportunity_id)) AS diff
FROM vw_app
WHERE account_created_date < current_date()
--  AND level_code = "UG"
;

-- COMMAND ----------

-- DBTITLE 1,Recommend
create or replace temporary view vw_recommend 
as
select 
  a.*
, b.recommend_flag
, b.recommend_date
, b.recommend_datetime
, b.recommend_product_category
, b.recommend_product_type
, b.recommend_reason
, b.recommend_products
, b.recommend_product1
, b.recommend_product2
, b.recommend_product3
, b.recommend_stage
, b.trailheads_submit_transcripts
, b.care_status_at_recommend
, b.not_admitted_at_rec
, b.enroll_counselor_at_rec
, datediff(b.recommend_date, a.app_date) AS days_app_to_rec
from vw_app a
left join users.jonathan_huck.inf_uex_recommend b
  on a.opportunity_id = b.opportunity_id
  and b.recommend_date >= a.account_created_date
;

select * from vw_recommend limit 100;

-- COMMAND ----------

-- DBTITLE 1,Degree Start
create or replace temporary view vw_degree_start
as
with unique_degree_starts as (
  select
    a.opportunity_id
  , b.start_date as degree_start_date
  , 1 as degree_start_flag
  --, b.academy_status
  --, b.academy_completed
  --, b.academy_attended
  --, b.day45_flag
  --, b.matric_date
  from vw_recommend a
  join users.jonathan_huck.inf_uex_degree_start b 
    on b.opportunity_id = a.opportunity_id
    and b.start_date >= a.app_date
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

select * from users.jonathan_huck.inf_uex_academy limit 100

-- COMMAND ----------

-- DBTITLE 1,On-Ramp Start
create or replace temporary view vw_onramp_start
as
with unique_onramp_starts as (
  select
    a.opportunity_id
  , 1 as acad_start_flag
  , b.acad_start_date 
  , b.referral_channel
  , b.offering_type
  , b.offering_subtype
  , b.product_code
  , b.product_name
  , b.partnership
  , b.client_subclass
  , b.acad_complete_flag
  , b.acad_complete_date
  , b.expiration_date
  , b.acad_drop_flag
  , b.acad_drop_date
  , b.acad_drop_7d_flag
  , b.acad_drop_72hr_flag
  from vw_degree_start a
  join users.jonathan_huck.inf_uex_academy b 
    on b.opportunity_id = a.opportunity_id
    and b.acad_start_date >= a.recommend_date -- FILTERS ONLY RECOMMENDED
  qualify row_number() over (
    partition by a.opportunity_id
    order by b.acad_start_date asc
  ) = 1
)

select
  a.*
, ifnull(b.acad_start_flag, 0) as acad_start_flag
, case 
    when degree_start_flag = 1 and acad_start_flag != 1 then 1 
    else 0 end as direct_degree_start_flag
, case 
    when degree_start_flag = 1 and acad_start_flag = 1 then 1 
    else 0 end as degree_start_with_acad_flag
, b.acad_start_date 
, b.referral_channel
, b.offering_type
, b.offering_subtype
, b.product_code
, b.product_name
, b.partnership
, b.client_subclass
, b.acad_complete_flag
, b.acad_complete_date
, b.acad_drop_flag
, b.acad_drop_date
, b.acad_drop_7d_flag
, b.acad_drop_72hr_flag
-- conversion times
, datediff(b.acad_start_date, a.recommend_date) AS days_rec_to_acad_start
, datediff(b.acad_complete_date, b.acad_start_date) AS days_acad_start_to_compl
, datediff(b.acad_drop_date, b.acad_start_date) AS days_acad_start_to_drop
, datediff(a.degree_start_date, b.acad_complete_date) AS days_acad_compl_to_degree_start
from vw_degree_start a
left join unique_onramp_starts b using (opportunity_id);

select * from vw_onramp_start 
where acad_start_flag = 1 limit 1000;

-- COMMAND ----------

-- DBTITLE 1,Transcript
CREATE OR REPLACE TEMPORARY VIEW vw_transcript
AS
WITH cte_transcript AS (
  SELECT DISTINCT 
    OPPORTUNITY__C as opportunity_id
  , MAX(CASE 
        WHEN degreetype__c LIKE 'Doctor%' THEN 1
        WHEN degreetype__c LIKE 'Mast%' THEN 1
        WHEN degreetype__c LIKE 'Bach%' THEN 1
        WHEN degreetype__c LIKE 'Assoc%' THEN 1
        ELSE 0 END) AS has_degree
    --max(CASE
    --      WHEN DEGREETYPE__C IN (
    --        'Associate of Science',
    --        'Associate of Applied Arts',
    --        "Bachelor's",
    --        "Master's",
    --        'Associate of Arts',
    --        'Doctorate',
    --        'Associate of Applied Science',
    --        'Associate – Other'
    --      ) THEN 2
    --      WHEN DEGREETYPE__C IN ('Other', 'No Degree Earned') THEN 1
    --    ELSE 0
    --END ) OVER (partition by OPPORTUNITY__C) AS degree_category
  FROM wgu_lakehouse.salesforce_srm.bi_t_studenttranscript
  GROUP BY 1
)

SELECT 
  a.*
--, --b.associate_plus
--, --b.no_degree
, c.has_degree
, b.courses_failed
, b.courses_withdrawn
, b.courses_passed
, b.courses_attempted
, b.transfer_credits
, b.not_transfer_credits
, b.gpa_all_crs
, b.gpa_tran_crs
, b.gpa_hs
FROM vw_onramp_start a
LEFT JOIN users.jonathan_huck.inf_uex_transcript b USING (opportunity_id)
LEFT JOIN cte_transcript c USING (opportunity_id);

SELECT 
  count(opportunity_id) AS ids
, count(distinct opportunity_id) AS unique_ids
, abs(count(opportunity_id) - count(distinct opportunity_id)) AS diff
FROM vw_transcript
WHERE account_created_date < current_date()

-- COMMAND ----------

-- DBTITLE 1,Admissions
CREATE OR REPLACE TEMPORARY VIEW vw_admission
AS
WITH nayt AS (
  SELECT 
    a.opportunity_id
  , to_date(from_utc_timestamp(fh.createddate, 'America/Denver')) AS care_status_date
  , fh.newvalue AS care_status
  , CASE 
      WHEN fh.newvalue LIKE "NAYT%" THEN 1
      WHEN fh.newvalue LIKE "NAMT%" THEN 1
      WHEN fh.newvalue LIKE "PEND%" THEN 1
      ELSE 0 END AS nayt_ever_flag
  FROM vw_transcript a
  JOIN wgu_lakehouse.salesforce_srm.bi_o_opportunityfieldhistory fh
    ON fh.opportunityid = a.opportunity_id
    AND fh.createddate <= a.app_date
  WHERE fh.field = "CareStatus__c"
  QUALIFY row_number() OVER (
    PARTITION BY a.opportunity_id
    ORDER BY fh.createddate DESC
  ) = 1
),

intv AS (
  SELECT 
    a.opportunity_id
  , to_date(from_utc_timestamp(fh.createddate, 'America/Denver')) AS care_status_date
  , fh.newvalue AS care_status
  , CASE 
      WHEN fh.newvalue LIKE "INTV%" THEN 1
      ELSE 0 END AS intv_flag
  FROM vw_transcript a
  JOIN wgu_lakehouse.salesforce_srm.bi_o_opportunityfieldhistory fh
    ON fh.opportunityid = a.opportunity_id
    AND fh.createddate <= a.app_date
  WHERE fh.field = "CareStatus__c"
  QUALIFY row_number() OVER (
    PARTITION BY a.opportunity_id
    ORDER BY fh.createddate DESC
  ) = 1
)

SELECT 
  a.*
, b.nayt_ever_flag
, c.intv_flag
, IF(b.nayt_ever_flag = 1, b.care_status_date, NULL) AS nayt_ever_date
, IF(c.intv_flag = 1, c.care_status_date, NULL) AS intv_date
FROM vw_transcript a
LEFT JOIN nayt b USING (opportunity_id)
LEFT JOIN intv c USING (opportunity_id)
;

SELECT 
  count(opportunity_id) AS ids
, count(distinct opportunity_id) AS unique_ids
, abs(count(opportunity_id) - count(distinct opportunity_id)) AS diff
FROM vw_admission
WHERE account_created_date < current_date()

-- COMMAND ----------

-- DBTITLE 1,Success
create or replace temporary view vw_success
as
WITH success AS (
  SELECT 
    b.*
  --, case when b.faa_date != a.degree_start_date then 1 else 0 end as close_faa_mismatch 
  FROM vw_admission a
  JOIN users.jonathan_huck.inf_uex_degree_success b
    ON b.student_pidm = a.student_pidm
    AND to_date(b.faa_date) >= to_date(a.app_date)
    --AND to_date(s.faa_date) >= to_date(ns.degree_start_date)
  QUALIFY row_number() OVER (
    PARTITION BY a.student_pidm
    ORDER BY faa_date ASC
  ) = 1  
)

SELECT 
  t.*
, s.faa_date
--, s.close_faa_mismatch --why are there ~1K of these?
, s.grad_flag
, s.grad_date
, s.drop_flag
, s.drop_date
, s.drop_type
, s.drop_sub_type
, s.drop_t1_flag
, s.drop_t2_flag
, s.retention_7months
, s.retention_13months
, s.r7_date
, s.r13_date
, s.limit_prog_t1
, s.limit_prog_t2
, s.earned_cus_t1
, s.earned_cus_t2
, s.t1_otp
, s.t2_otp
, datediff(degree_start_date, drop_date) AS days_degree_start_to_drop
, datediff(degree_start_date, grad_date) AS days_degree_start_to_complete
FROM vw_admission t
LEFT JOIN success s USING (student_pidm);

-- COMMAND ----------

-- DBTITLE 1,Transform
CREATE OR REPLACE TEMPORARY VIEW vw_transform
AS 
SELECT *

-- App-to-Enroll Conversion
, CASE 
    WHEN days_app_to_degree_start <= 30 THEN '0-30 days' 
    WHEN days_app_to_degree_start > 30 AND days_app_to_degree_start <= 60 THEN '31-60 days' 
    WHEN days_app_to_degree_start > 60 AND days_app_to_degree_start <= 90 THEN '61-90 days'
    WHEN days_app_to_degree_start > 90 THEN '90+ days' 
    ELSE NULL END AS bins_app_to_degree_start

-- App to Recommendation
, CASE 
    WHEN days_app_to_rec <= 7 THEN '0-7 days' 
    WHEN days_app_to_rec > 7  AND days_app_to_rec <= 14 THEN '8-14 days' 
    WHEN days_app_to_rec > 14 AND days_app_to_rec <= 30 THEN '14-30 days' 
    WHEN days_app_to_rec > 30 AND days_app_to_rec <= 60 THEN '31-60 days' 
    WHEN days_app_to_rec > 60 AND days_app_to_rec <= 90 THEN '61-90 days'
    WHEN days_app_to_rec > 90 THEN '90+ days' 
    ELSE NULL END AS bins_app_to_rec

-- Recommendation to On-Ramp Start
, CASE 
    WHEN days_rec_to_acad_start <= 7 THEN '0-7 days' 
    WHEN days_rec_to_acad_start > 7  AND days_rec_to_acad_start <= 14 THEN '8-14 days' 
    WHEN days_rec_to_acad_start > 14 AND days_rec_to_acad_start <= 30 THEN '14-30 days' 
    WHEN days_rec_to_acad_start > 30 AND days_rec_to_acad_start <= 60 THEN '31-60 days' 
    WHEN days_rec_to_acad_start > 60 AND days_rec_to_acad_start <= 90 THEN '61-90 days'
    WHEN days_rec_to_acad_start > 90 THEN '90+ days' 
    ELSE NULL END AS bins_rec_to_acad_start

-- On-Ramp Start to Complete
, CASE 
    WHEN days_acad_start_to_compl <= 30 THEN '0-30 days' 
    WHEN days_acad_start_to_compl > 30 AND days_acad_start_to_compl <= 60 THEN '31-60 days' 
    WHEN days_acad_start_to_compl > 60 AND days_acad_start_to_compl <= 90 THEN '61-90 days'
    WHEN days_acad_start_to_compl > 90 THEN '90+ days' 
    ELSE NULL END AS bins_acad_start_to_complete

-- On-Ramp Start to Drop
, CASE 
    WHEN days_acad_start_to_drop <= 30 THEN '0-30 days' 
    WHEN days_acad_start_to_drop > 30 AND days_acad_start_to_drop <= 60 THEN '31-60 days' 
    WHEN days_acad_start_to_drop > 60 AND days_acad_start_to_drop <= 90 THEN '61-90 days'
    WHEN days_acad_start_to_drop > 90 THEN '90+ days' 
    ELSE NULL END AS bins_acad_start_to_drop

-- On-Ramp Complete to Degree Start
, CASE 
    WHEN days_acad_compl_to_degree_start <= 7 THEN '0-7 days' 
    WHEN days_acad_compl_to_degree_start > 7  AND days_acad_compl_to_degree_start <= 14 THEN '8-14 days' 
    WHEN days_acad_compl_to_degree_start > 14 AND days_acad_compl_to_degree_start <= 30 THEN '14-30 days' 
    WHEN days_acad_compl_to_degree_start > 30 AND days_acad_compl_to_degree_start <= 60 THEN '31-60 days' 
    WHEN days_acad_compl_to_degree_start > 60 AND days_acad_compl_to_degree_start <= 90 THEN '61-90 days'
    WHEN days_acad_compl_to_degree_start > 90 THEN '90+ days' 
    ELSE NULL END AS bins_acad_compl_to_degree_start

-- Degree Start to Completion
, CASE 
    WHEN days_degree_start_to_complete <= 30 THEN '0-30 days' 
    WHEN days_degree_start_to_complete > 30 AND days_degree_start_to_complete <= 60 THEN '31-60 days' 
    WHEN days_degree_start_to_complete > 60 AND days_degree_start_to_complete <= 90 THEN '61-90 days'
    WHEN days_degree_start_to_complete > 90 THEN '90+ days' 
    ELSE NULL END AS bins_degree_start_to_complete

-- Degree Start to Drop
, CASE 
    WHEN days_degree_start_to_drop <= 30 THEN '0-30 days' 
    WHEN days_degree_start_to_drop > 30 AND days_degree_start_to_drop <= 60 THEN '31-60 days' 
    WHEN days_degree_start_to_drop > 60 AND days_degree_start_to_drop <= 90 THEN '61-90 days'
    WHEN days_degree_start_to_drop > 90 THEN '90+ days' 
    ELSE NULL END AS bins_degree_start_to_drop
FROM vw_success
;

select * from vw_transform limit 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("vw_transform")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.tvz_uex_cohort_account")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analysis
-- MAGIC

-- COMMAND ----------

select 
  opportunity_id
, student_pidm
, account_created_date
, recommend_date
, college_code
, level_code
, program_code
, recommend_reason
, recommend_products
, recommend_stage
, care_status_latest
, care_status_at_recommend
, transfer_eval_status
, first_eval_sent
, transfer_eval_app_complete

, name
, email
from vw_recommend
where recommend_stage = 'Pre-Transcript'
  and sse = "SSE"
  and recommend_date > '2024-08-22'
  and recommend_date < current_date()

-- COMMAND ----------

select 
  account_created_date, app_date, recommend_date, care_status_at_recommend, recommend_stage

from vw_recommend
where recommend_flag = 1 --recommend_stage = 'Pre-Transcript'
  and sse = "SSE"
  and app_Date is null
