-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Trailheads Academy Inquiries
-- MAGIC Cohorts defined by the date Academy receives a new inquiry in their system, filtered on Trailheads client subclass.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recommendation Received Cohort

-- COMMAND ----------

-- DBTITLE 1,Academy Inquiries
CREATE OR REPLACE TEMPORARY VIEW vw_academy 
AS
WITH academy_inquiries AS (
  SELECT 
    IF(a.wgu_id__c = 'null', NULL, a.wgu_id__c) AS wgu_id
  , a.id AS contact_id
  , b.opportunity_id
  , b.pidm__c AS student_pidm
  , 1 AS recommend_received
  , to_date(a.createddate) AS received_date

    -- weekly date fields start on Monday end on Sunday
    --, DATE_ADD(a.createddate, -weekday(a.createddate)) AS received_week_start
  , DATE_ADD(a.createddate, -weekday(a.createddate) + 6) AS received_week
  , LAST_DAY(a.createddate) AS received_month
    
  , CASE 
      WHEN b.opportunity_id IS NULL THEN 'Unmatched Opportunity' 
      ELSE b.opportunity_id END AS referral_opportunity
  , a.Academy_Referral_Reason__c AS referral_reason
    -- record can have multiple subclass tags, including Trailheads
  , IF(a.client_subclass__c LIKE '%Trailheads%', 'Trailheads', NULL) AS client_subclass

  , a.Product_Type__c AS offering_subtype
  , CASE
      WHEN a.Product_Type__c IN ('Single Course', 'Course Bundle') THEN 'On-Ramp'
      WHEN a.Product_Type__c = 'null' THEN NULL
      ELSE a.Product_Type__c END AS offering_type
  , CASE
      WHEN a.Academy_Program_of_Interest__c = 'Business' THEN 'WSB'
      WHEN a.Academy_Program_of_Interest__c = 'Teaching' THEN 'WSE'
      WHEN a.Academy_Program_of_Interest__c = 'Health' THEN 'LSH'
      WHEN a.Academy_Program_of_Interest__c IN ('Computer Science', 'Technology', 'Information Technology') THEN 'WST'
      WHEN a.Academy_Program_of_Interest__c = 'Certificates' THEN 'Certificates'
    ELSE NULL END AS acad_program_of_interest

  , IF(b.acad_enrollment_date = 'null', NULL, b.acad_enrollment_date) AS acad_start_date
  --DATE_ADD(b.acad_enrollment_date, -weekday(b.acad_enrollment_date)) AS acad_enroll_week_start,
  , DATE_ADD(b.acad_enrollment_date, -weekday(b.acad_enrollment_date) + 6) AS acad_start_week
  , LAST_DAY(b.acad_enrollment_date) AS acad_start_month

  , CASE 
      WHEN b.Acad_Enroll_Status IS NULL THEN 'Prospecting' 
      ELSE b.Acad_Enroll_Status END AS acad_enroll_status
  , b.faa_date -- this is screening inquiries who went direct to degree if you join on this
  , b.WGU_Enrolled_Program
  , b.WGU_Status
  
  FROM wguacademy.contacts a
  LEFT JOIN wguacademy.stg_match_wgu_academy b 
    ON a.id = b.acad_contact_id AND b.MinMatchReferSeq = TRUE
  WHERE a.client_subclass__c LIKE '%Trailheads%' --a.Client_Subclass__c = 'Trailheads'
    AND a.createddate >= '2024-01-22' --exclude pre-Trailheads records
    AND a.test_record__c != TRUE
)

SELECT 
  a.*
-- this join created some duplicates in the original -- 24/10/16
, b.collegeinterest__c AS wgu_college_of_interest
, b.wgu_program_of_interest
FROM academy_inquiries a
LEFT JOIN wguacademy.stg_match_wgu_academy b 
 ON a.contact_id = b.acad_contact_id AND b.MinMatchWGUEnrollSeq = TRUE
;

SELECT * FROM vw_academy;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM vw_academy

-- COMMAND ----------

-- DBTITLE 1,Check Starts
SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM vw_academy
WHERE acad_start_date IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Check Match
SELECT 
  CASE WHEN contact_id IS NULL THEN 1 ELSE 0 END AS missing_contact
, CASE WHEN opportunity_id IS NULL THEN 1 ELSE 0 END AS missing_oppo
, COUNT(*) -- about 1% missing opportunity ID as of October 18
FROM vw_academy
GROUP BY ALL

-- COMMAND ----------

-- DBTITLE 1,Academy MBR
WITH inquiries AS (
  SELECT 
    wbr.contact_id
  , wbr.reportdate
  , to_date(date_trunc("week", wbr.reportdate)) AS report_week
  , to_date(last_day(wbr.reportdate)) AS report_month
  , wbr.client_subclass__c 
  FROM wguacademy.tvz_wbr_compendium_tempv3 wbr
  LEFT JOIN wguacademy.contacts c ON wbr.contact_id = c.id
  WHERE denominator <> 0
    AND wbr.index IS NOT NULL
    AND wbr.metric = "5_inquiries"
    AND wbr.client IS NULL
    AND c.client_subclass__c = "Trailheads"
),

wbr AS (
  SELECT
    report_month AS report_date
  , count(contact_id) AS n_wbr
  FROM inquiries
  GROUP BY 1
),

etl AS (
  SELECT
    last_day(received_date) AS report_date
  , COUNT(contact_id) AS n_etl
  FROM vw_academy
  GROUP BY 1
)

SELECT
  a.report_date
, a.n_etl
, b.n_wbr
, a.n_etl - b.n_wbr AS n_diff
, round((a.n_etl - b.n_wbr) / a.n_etl, 2) AS pct_diff
FROM etl a
JOIN wbr b USING (report_date)
WHERE report_date < current_date()
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Academy Product

-- COMMAND ----------

-- DBTITLE 1,Enrollment
CREATE OR REPLACE TEMPORARY VIEW vw_product
AS
WITH course_enrollment AS (
  SELECT DISTINCT
    wgu_id
  , enrollment_start_date AS product_enroll_start_date
  , enrollment_end_date AS product_enroll_end_date
  , course_id
  , CASE 
      WHEN item_name = 'Single Course' THEN CONCAT('Single Course: ', course_name)
      ELSE item_name END AS enrolled_product
  , CASE 
      WHEN item_name = 'Single Course' THEN 'Single Course'
      WHEN item_name LIKE 'Bundle%' THEN 'Course Bundle'
      WHEN item_name LIKE 'Guaranteed%' THEN 'Course Bundle'
      ELSE "Certificate" END AS enrolled_product_subtype
  , CASE 
      WHEN item_name = 'Single Course' THEN 'On-Ramp'
      WHEN item_name LIKE 'Bundle%' THEN 'On-Ramp'
      WHEN item_name LIKE 'Guaranteed%' THEN 'On-Ramp'
      ELSE "Certificate" END AS enrolled_product_type
  FROM wguacademy2.vw_course_enrollments 
  WHERE course_name != "Ready for Success" -- exclude orientation
  -- choose earliest enrollment
  QUALIFY row_number() OVER (
    PARTITION BY wgu_id
    ORDER BY enrollment_start_date
  ) = 1
),

subscription_length AS (
  SELECT DISTINCT
    product_sku
  --, subscription_name
  , subscription_trial_length
  , subscription_trial_period 
  FROM wguacademy2.woocommerce_transactions 
  WHERE test_record__c = FALSE
  GROUP BY ALL
),

completed AS (
  SELECT DISTINCT
    wgu_id
  , course_id
  , course_complete_date AS completed_course_date
  , 1 AS completed_course_flag
  FROM wguacademy.tvz_acad_2_report 
  WHERE course_complete_date is not null
)

SELECT 
  a.*
, product_enroll_start_date
, product_enroll_end_date
, enrolled_product
, enrolled_product_type
, enrolled_product_subtype
, completed_course_flag
, completed_course_date
FROM vw_academy a
LEFT JOIN course_enrollment c ON a.wgu_id = c.wgu_id
LEFT JOIN subscription_length s ON c.course_id = s.product_sku
LEFT JOIN completed g ON a.wgu_id = g.wgu_id AND c.course_id = g.course_id
;

select * from vw_product

-- COMMAND ----------

-- DBTITLE 1,Check IDs
-- check if these are the same 
SELECT
  count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM vw_product

-- COMMAND ----------

-- DBTITLE 1,Check Missing Product
SELECT
  CASE WHEN enrolled_product IS NULL THEN 0 ELSE 1 END AS has_product
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM vw_product
WHERE acad_start_date IS NOT NULL
GROUP BY 1

-- COMMAND ----------

-- DBTITLE 1,Enrollment Date Mismatch
SELECT 
  wgu_id
, acad_start_date
, product_enroll_start_date
, abs(datediff(product_enroll_start_date, acad_start_date)) AS difference
, enrolled_product
FROM vw_product
WHERE acad_start_date != product_enroll_start_date
ORDER BY difference DESC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## WGU System

-- COMMAND ----------

-- DBTITLE 1,Demographics
CREATE OR REPLACE TEMPORARY VIEW vw_demo
AS
WITH applications AS (
  SELECT DISTINCT
    opportunity_id
  , status_date AS app_date
  , college_code
  , level_code
  , program_code
  , is_sse_student__c AS sse
  FROM wgubi.vw_rst_marketing --wgu_analytics.marketing.vw_rst_marketing
  WHERE app_complete_notpaid = 1
),

demographics AS (
  SELECT 
    a.*
  , d.gender AS gender
  , d.dob
  , datediff(a.app_date, to_date(d.dob)) AS age
  , CASE
      WHEN d.under_rep_minority = "Y" THEN "BHI2+"
      WHEN d.under_rep_minority = "N" THEN "Not BHI2+" 
      ELSE NULL END AS bhi2
  , d.ethnicity_reported AS ethnicity
  , d.wgu_region AS region
  , d.dma_name
  , d.state
  , CASE 
      WHEN d.annual_income IN ('Unknown', 'Not reported', 'iWouldRatherNotRespond') THEN 'Missing'
      WHEN d.annual_income IS NULL THEN 'Missing'
      ELSE d.annual_income END AS income
  , CASE 
      WHEN d.household_income_level IN ('Declined', 'Not reported') THEN 'Missing'
      WHEN d.household_income_level IS NULL THEN 'Missing'
      ELSE d.household_income_level END AS income_level
  , CASE 
      WHEN d.ftft_preliminary = "Y" THEN "FTFT"
      WHEN d.ftft_preliminary = "N" THEN "Not FTFT"
      ELSE NULL END AS ftft
  , CASE 
      WHEN d.marital_status IN ('Single', 'Married', 'Divorced', 'Separated', 'Widowed') THEN d.marital_status
      ELSE 'Missing' END AS marital_status
  , INT(non_term_applicant) AS non_term_applicant
  FROM applications a
  LEFT JOIN wgubi.dim_lead_demographics d --wgu_lakehouse.demographics.dim_lead_demographics
    ON d.opportunity_id = a.opportunity_id
)

SELECT 
  p.*
, d.app_date
, d.college_code AS college_code_app
, d.level_code AS level_code_app

-- replace NULL values with 'Missing' text
, IF(ISNULL(d.sse), "Missing", d.sse) AS sse
, IF(ISNULL(d.age), "Missing", d.age) AS age
, IF(ISNULL(d.gender), "Missing", d.gender) AS gender
, IF(ISNULL(d.ethnicity), "Missing", d.ethnicity) AS ethnicity
, IF(ISNULL(d.bhi2), "Missing", d.bhi2) AS bhi2
, IF(ISNULL(d.income), "Missing", d.income) AS income
, IF(ISNULL(d.income_level), "Missing", d.income_level) AS income_level
, IF(ISNULL(d.marital_status), "Missing", d.marital_status) AS marital_status
, IF(ISNULL(d.region), "Missing", d.region) AS region
, IF(ISNULL(d.ftft), "Missing", d.ftft) AS ftft
, IF(ISNULL(d.dma_name), "Missing", d.dma_name) AS dma_name
, IF(ISNULL(d.state), "Missing", d.state) AS state
FROM vw_product p
LEFT JOIN demographics d USING (opportunity_id);

SELECT * FROM vw_demo 
LIMIT 1000

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT
  count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM vw_demo

-- COMMAND ----------

-- DBTITLE 1,Check Dates
-- How many records with negative app-to-enroll times?
SELECT 
  opportunity_id
, app_date
, acad_start_date
FROM vw_demo
WHERE acad_start_date < app_date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cohort Metrics

-- COMMAND ----------

-- DBTITLE 1,Pivot
CREATE OR REPLACE TEMPORARY VIEW vw_cohort
AS
WITH rst_student AS (
  SELECT DISTINCT
    s.student_pidm
  , s.faa_date as degree_start_date
  , CASE 
      WHEN s.college_code = 'BU' THEN 'WSB'
      WHEN s.college_code = 'HE' THEN 'LSH'
      WHEN s.college_code = 'IT' THEN 'WST'
      WHEN s.college_code = 'TC' THEN 'WSE'
      END AS school_code_start
  FROM wgubi.vw_rst_student s
  JOIN vw_demo a 
    ON a.student_pidm = s.student_pidm
    AND a.received_date < s.faa_date
  WHERE s.new_start = 1
    and year(s.faa_date) = 2024
  QUALIFY row_number() OVER (
    PARTITION BY s.student_pidm
    ORDER BY s.faa_date
  ) = 1
)

select
  a.*
, datediff(acad_start_date, received_date) AS days_rec_onramp_start
, CASE
    WHEN datediff(acad_start_date, received_date) <= 7 THEN "7 or fewer days"
    WHEN datediff(acad_start_date, received_date) between 8 and 14 THEN "8-14 days"
    WHEN datediff(acad_start_date, received_date) between 15 and 30 THEN "15-30 days"
    WHEN datediff(acad_start_date, received_date) between 31 and 60 THEN "31-60 days"
    WHEN datediff(acad_start_date, received_date) > 60 THEN "61+ days"
    ELSE NULL END AS bins_rec_onramp_start

, case when school_code_start = 'WSB' then 1 else 0 end as WSB
, case when school_code_start = 'WSE' then 1 else 0 end as WSE
, case when school_code_start = 'LSH' then 1 else 0 end as LSH
, case when school_code_start = 'WST' then 1 else 0 end as WST

-- Flags for enrollment funnel stages
, case when acad_start_date is not null then 1 else 0 end as Started_OnRamp
, case when acad_enroll_Status LIKE 'Enrolled%' then 1 else 0 end as Active_OnRamp_Total
, case when acad_enroll_status = 'Enrolled' then 1 else 0 end as Active_OnRamp_OnTime
, case when acad_enroll_status = 'Enrolled - Month to Month' then 1 else 0 end as Active_OnRamp_Monthly
, case when acad_enroll_status = 'Paused' then 1 else 0 end as Paused_OnRamp -- On Hold?
, case when acad_enroll_status = 'Graduated' then 1 else 0 end as Completed_OnRamp
, case when acad_enroll_status IN ('Cancelled', 'Withdrawn', 'Reverse Enrollment') then 1 else 0 end as Dropped_OnRamp
, case when acad_enroll_status IN ('Reverse Enrollment') then 1 else 0 end as Dropped_OnRamp_7d
, case when acad_enroll_status IN ('Cancelled', 'Withdrawn') then 1 else 0 end as Dropped_OnRamp_7d_over
, case when degree_start_date is not null then 1 else 0 end as Started_Degree
, case when wgu_status = 'AS' then 1 else 0 end as Active_Degree
, case when wgu_status IN ('DR', 'WI') then 1 else 0 end as Dropped_Degree
, case when wgu_status = 'GR' then 1 else 0 end as Graduated_Degree
, case when acad_start_date is null and degree_start_date is not null then 1 else 0 end as Started_Direct_Degree
, case when acad_start_date is not null and degree_start_date is not null then 1 else 0 end as Started_OnRamp_and_Degree
, current_timestamp() as last_refreshed
from vw_demo a
left join rst_student rst1 USING (student_pidm)
;

SELECT * FROM vw_cohort;

-- COMMAND ----------

-- DBTITLE 1,Check Completion Inconsistency
SELECT
  CASE WHEN Completed_OnRamp = 1 THEN 1 ELSE 0 END AS academy_status_code,
  CASE WHEN completed_course_flag = 1 THEN 1 ELSE 0 END AS product_complete_code,
  COUNT(*)
FROM vw_cohort
WHERE Completed_OnRamp != completed_course_flag
GROUP BY ALL

-- COMMAND ----------

-- DBTITLE 1,School Counts
WITH recommend AS (
  SELECT
    acad_program_of_interest as college_code
  , to_date(received_month) AS report_month
  , int(count(opportunity_id)) AS n --nayt_recommends
  FROM vw_cohort
  WHERE received_month >= '2023-07-01'
  GROUP BY 1, 2
)

SELECT
  report_month
, SUM(CASE WHEN college_code = 'LSH' THEN n ELSE 0 END) AS LSH
, SUM(CASE WHEN college_code = 'WSB' THEN n ELSE 0 END) AS WSB
, SUM(CASE WHEN college_code = 'WSE' THEN n ELSE 0 END) AS WSE
, SUM(CASE WHEN college_code = 'WST' THEN n ELSE 0 END) AS WST
, SUM(CASE WHEN college_code NOT IN ('LSH', 'WSB', 'WSE', 'WST') THEN n ELSE 0 END) AS Missing
, SUM(n) AS WGU
FROM recommend
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write

-- COMMAND ----------

-- DBTITLE 1,Write
-- MAGIC %python
-- MAGIC (spark.sql('select * from vw_cohort')
-- MAGIC .write.option("overwriteSchema", "true")
-- MAGIC .saveAsTable(name='wgubidev.inf_uex_academy_inquiry',
-- MAGIC              path='s3a://ir-edw/wgubidev/inf_uex_academy_inquiry',
-- MAGIC              mode = 'overwrite',
-- MAGIC              format='delta',
-- MAGIC              partitionby='received_month')) 
