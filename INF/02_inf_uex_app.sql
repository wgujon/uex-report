-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Application Complete

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Application

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_app
AS
WITH applications AS (
  SELECT DISTINCT
    opportunity_id
  , to_date(status_date) AS app_date
  , status_date AS app_datetime
  , college_code
  , level_code
  , program_code
  , enrollment_counselor
  , IF(is_sse_student__c = 1, "SSE", "Legacy") AS sse
  FROM wgu_analytics.marketing.vw_rst_marketing
  WHERE app_complete_notpaid = 1
),

level_3 AS (
  SELECT DISTINCT
    level_3 AS program_group
  , program_code
  FROM wgubi.vw_de_finance_program_grouping
  WHERE report_date = (
    SELECT MAX(report_date) 
    FROM wgubi.vw_de_finance_program_grouping
  )
)

SELECT 
  o.id AS opportunity_id
, CASE WHEN o.pidm__c = "" THEN NULL ELSE o.pidm__c END AS student_pidm
, CASE WHEN con.wgu_id__c = "" THEN NULL ELSE con.wgu_id__c END AS wgu_id
, 1 AS app_flag
, a.app_date
, a.app_datetime
, a.college_code
, a.level_code 
, a.program_code
, l3.program_group
, a.enrollment_counselor
, a.sse
, o.carestatus__c AS care_status_latest
, o.goodbadlead__c 
, o.badleadreason__c

--, o.outstandingtransferevaluations__c AS outstanding_transfer_evals
--, o.transferevaluationstatus__c AS transfer_eval_status
--, o.FIRSTEVALSENT__C AS first_eval_sent
--, o.TRANSFEREVALUATIONAPPLICATIONCOMPLETEDT__C AS transfer_eval_app_complete
--, o.completedtransferevaluations__c AS completed_transfer_evals
--, CASE 
--    WHEN o.academy_student_contact_record_type__c = '' THEN NULL
--    ELSE o.academy_student_contact_record_type__c END AS academy_contact_record_type

, trim(lower(o.name)) AS name
, trim(lower(o.contactemail__c)) AS email
FROM wgu_lakehouse.salesforce_srm.bi_o_opportunity o
JOIN applications a ON a.opportunity_id = o.id
LEFT JOIN wgu_lakehouse.salesforce_srm.bi_c_contact con ON o.studentcontact__c = con.id
LEFT JOIN level_3 l3 ON l3.program_code = a.program_code
;

SELECT * FROM vw_app LIMIT 1000;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT
  count(opportunity_id) as n
, count(distinct opportunity_id) as unique_n
, count(opportunity_id) - count(distinct opportunity_id) as diff
from vw_app

-- COMMAND ----------

-- DBTITLE 1,MBR
with etl as (
  select
    last_day(app_date) as app_month
  , count(opportunity_id) as n_etl
  from vw_app
  group by 1
),

mbr as (
  select
    to_date(end_date) as app_month
  , int(sum(app_complete_notpaid)) as n_mbr
  from wgu_analytics.executive_report.dbt_exe_apps_complete
  where time_period = "Monthly"
  group by 1
)

select 
  etl.app_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (app_month)
where app_month < current_date()
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,Keep Good Leads
-- MBR includes bad leads and test records but we don't want this
-- This filter mismatches MBR
CREATE OR REPLACE TEMPORARY VIEW vw_app_good
AS
SELECT * FROM vw_app
WHERE badleadreason__c != 'Test Lead'
  AND goodbadlead__c != 'Bad Lead'
  AND lower(name) NOT LIKE 'test%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Demographics

-- COMMAND ----------

-- DBTITLE 1,Function
-- MAGIC %run
-- MAGIC "/Workspace/Users/jonathan.huck@wgu.edu/UEX Reporting/INF/fun_demographics"

-- COMMAND ----------

-- DBTITLE 1,Join Demographics
-- MAGIC %python
-- MAGIC df = join_demographics("vw_app_good", date_col = "app_date")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("mergeSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_app")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validate

-- COMMAND ----------

DESCRIBE users.jonathan_huck.inf_uex_app;

-- COMMAND ----------

-- DBTITLE 1,MBR
-- minor differences are due to screening test leads and bad leads
with etl as (
  select
    last_day(app_date) as app_month
  , count(opportunity_id) as n_etl
  from users.jonathan_huck.inf_uex_app
  group by 1
),

mbr as (
  select
    to_date(end_date) as app_month
  , int(sum(app_complete_notpaid)) as n_mbr
  from wgu_analytics.executive_report.dbt_exe_apps_complete
  where time_period = "Monthly"
  group by 1
)

select 
  etl.app_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (app_month)
order by 1 desc
