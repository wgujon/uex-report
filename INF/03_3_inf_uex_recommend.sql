-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Trailheads Recommendations
-- MAGIC - ETL joins Trailheads fields from `bi_o_opportunity` to `inf_daily_trailhead_recommend` records and cleans up fields. 
-- MAGIC - Validate one record per opportunity ID (one recommendation per ID).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Recommendations

-- COMMAND ----------

select * from wgu_analytics.marketing.inf_daily_trailhead_recommend limit 100

-- COMMAND ----------

-- DBTITLE 1,Recommendations
CREATE OR REPLACE TEMPORARY VIEW vw_recommend
AS
WITH recommendations AS (
  SELECT
    r.opportunity_id
  , 1 AS recommend_flag
  , r.th_recommend_date AS recommend_datetime
  , to_date(r.th_recommend_date) AS recommend_date
  , CASE
      WHEN r.recommended_enrollment_pathway = "" THEN NULL
      WHEN r.recommended_enrollment_pathway = "6 Month Term" THEN NULL
      ELSE r.recommended_enrollment_pathway END AS recommend_product_type
  , r.academy_referral_reason AS recommend_reason
  , LEFT(r.care_status_at_recommend, 4) AS care_status_at_recommend
  , o.trailheadrecommendation__c AS recommend_products
  , trim(regexp_extract(o.trailheadrecommendation__c, '<li>([^<]+)</li>', 1)) AS recommend_product1
  , trim(regexp_extract(o.trailheadrecommendation__c, '<li>([^<]+)</li><li>([^<]+)</li>', 2)) AS recommend_product2
  , trim(regexp_extract(o.trailheadrecommendation__c, '<li>([^<]+)</li><li>([^<]+)</li><li>([^<]+)</li>', 3)) AS recommend_product3 
  , CASE
      WHEN o.trailheadsrecommendationstage__c = '' THEN NULL
      ELSE o.trailheadsrecommendationstage__c END AS recommend_stage
  -- whether or not applicant "chooses" pathway on SSE
  , CASE o.trailheadssubmittranscripts__c
      WHEN 0 THEN 0
      WHEN FALSE THEN 0
      WHEN 1 THEN 1
      WHEN TRUE THEN 1
      ELSE NULL END AS trailheads_submit_transcripts
      
  , LEFT(r.latest_care_status, 4) AS latest_care_status
  , r.college_code
  , r.level_code
  , r.program_code
  , r.wgu_region
  , ifnull(r.gender, "Missing") AS gender
  , ifnull(r.ethnicity_reported, "Missing") AS ethnicity
  , ifnull(r.under_rep_minority, "Missing") AS under_rep_minority
  , ifnull(r.military_standing, "Missing") AS military_standing
  , ifnull(r.annual_income, "Missing") AS annual_income

  FROM wgu_analytics.marketing.inf_daily_trailhead_recommend r
  LEFT JOIN wgu_lakehouse.salesforce_srm.bi_o_opportunity o ON r.opportunity_id = o.id
),

-- enrollment counselor assigned at time of recommendation
enrollment_counselor AS (
  SELECT
    a.opportunity_id
  , ec.enrollment_counselor AS enroll_counselor_at_rec
  FROM recommendations a 
  LEFT JOIN wgu_lakehouse.enrollment.inf_leads_enrollment_counselor ec
    ON ec.opportunity_id = a.opportunity_id
    AND ec.status_date <= a.recommend_date
  QUALIFY row_number() OVER (
    PARTITION BY a.opportunity_id
    ORDER BY ec.status_date DESC
  ) = 1
)

SELECT
  r.opportunity_id
, recommend_flag
, recommend_date
, recommend_datetime
, CASE 
    WHEN recommend_product_type = "" THEN NULL
    WHEN recommend_product_type IN ("Single Course", "Course Bundle") THEN "On-Ramp" 
    ELSE recommend_product_type END AS recommend_product_category
, CASE 
    WHEN recommend_product_type = "" THEN NULL
    WHEN recommend_product_type = "6 Month Term" THEN NULL
    ELSE recommend_product_type END AS recommend_product_type
, IF(recommend_reason = "", NULL, recommend_reason) AS recommend_reason
, IF(recommend_products = "", NULL, recommend_products) AS recommend_products
, IF(recommend_product1 = "", NULL, recommend_product1) AS recommend_product1
, IF(recommend_product2 = "", NULL, recommend_product2) AS recommend_product2
, IF(recommend_product3 = "", NULL, recommend_product3) AS recommend_product3
, recommend_stage
, trailheads_submit_transcripts
, care_status_at_recommend
, CASE 
    -- PEND is a special "not admitted" case in WST
    WHEN care_status_at_recommend IN ('NAMT', 'NAYT', 'PEND') THEN 1
    ELSE 0 END AS not_admitted_at_rec
, ec.enroll_counselor_at_rec

, r.latest_care_status
, r.college_code
, r.level_code
, r.program_code
--, r.wgu_region
--, r.gender
--, r.ethnicity
--, r.under_rep_minority
--, r.military_standing
--, r.annual_income

FROM recommendations r
LEFT JOIN enrollment_counselor ec USING (opportunity_id)
;

SELECT * FROM vw_recommend LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Application Questions

-- COMMAND ----------

select * from users.jonathan_huck.inf_uex_skill_conf limit 100

-- COMMAND ----------

create or replace temporary view vw_question
as
select 
  r.*
, sc.recommend_code
, pbq_hs_diploma
, pbq_assoc_degree_plus
, pbq_bachelor_degree
, pbq_bachelor_degree_plus
, pbq_college_gpa20_plus
, pbq_college_gpa25_plus
, pbq_gpa25_plus
, pbq_hsgpa25_plus
, sc.Math 
, sc.Writing
, sc.Resilience
, sc.Career
, sc.SC1
, sc.SC2
, sc.SC3
, sc.SC4
, sc.Product1
, sc.Product2
, sc.Product3
from vw_recommend r
left join users.jonathan_huck.inf_uex_personal_background pb using (opportunity_id)
left join users.jonathan_huck.inf_uex_skill_conf sc using (opportunity_id)
;

select * from vw_question;

-- COMMAND ----------

SELECT 
  COUNT(opportunity_id) AS ids
, COUNT(DISTINCT opportunity_id) AS unique_ids
FROM vw_question

-- COMMAND ----------

-- MAGIC %md ## Write

-- COMMAND ----------

-- DBTITLE 1,Write
-- MAGIC %python
-- MAGIC df = spark.table("vw_recommend")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("mergeSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_recommend")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validation

-- COMMAND ----------

-- DBTITLE 1,Data
DESCRIBE users.jonathan_huck.inf_uex_recommend

-- COMMAND ----------

-- DBTITLE 1,IDs
SELECT 
  COUNT(opportunity_id) AS ids
, COUNT(DISTINCT opportunity_id) AS unique_ids
FROM users.jonathan_huck.inf_uex_recommend

-- COMMAND ----------

-- DBTITLE 1,MBR
with etl as (
  SELECT 
    last_day(recommend_date) AS report_month
  , COUNT(opportunity_id) AS n_etl
  FROM users.jonathan_huck.inf_uex_recommend
  GROUP BY 1
),

mbr as (
  SELECT
    to_date(end_date) AS report_month
  , int(sum(n_recommends)) AS n_mbr
  FROM wgu_analytics.executive_report.dbv_exe_trailhead_recommend
  WHERE time_period = 'Monthly'
  GROUP BY 1
)

select 
  etl.report_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (report_month)
where report_month < current_date()
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,MBR - NAYT
with etl as (
  SELECT 
    last_day(recommend_date) AS report_month
  , COUNT(opportunity_id) AS n_etl
  FROM users.jonathan_huck.inf_uex_recommend
  WHERE care_status_at_recommend = 'NAYT'
  GROUP BY 1
),

mbr as (
  SELECT
    to_date(end_date) AS report_month
  , int(sum(nayt_recommends)) AS n_mbr
  FROM wgu_analytics.executive_report.dbv_exe_trailhead_recommend
  WHERE time_period = 'Monthly'
  GROUP BY 1
)

select 
  etl.report_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (report_month)
where report_month < current_date()
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,Products
SELECT
  recommend_product_type
, recommend_product1 
, COUNT(*) AS n
FROM vw_recommend
GROUP BY ALL
ORDER BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Reasons
SELECT 
  recommend_reason
, COUNT(*) AS n
FROM vw_recommend GROUP BY recommend_reason

-- COMMAND ----------

select * 
from users.jonathan_huck.inf_uex_recommend
where recommend_stage = 'Pre-Transcript'
  and recommend_date > '2024-08-22'
  and recommend_date < current_date()
