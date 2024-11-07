-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Account Created
-- MAGIC One record per account created (i.e., APIN or APNN)

-- COMMAND ----------

-- DBTITLE 1,Account
CREATE OR REPLACE TEMPORARY VIEW vw_account
AS
WITH level_3 AS (
  SELECT DISTINCT
    level_3 AS program_group
  , program_code
  FROM wgubi.vw_de_finance_program_grouping
  WHERE report_date = (
    SELECT MAX(report_date) 
    FROM wgubi.vw_de_finance_program_grouping
  )
)

SELECT DISTINCT
  vrm.opportunity_id
, IF(o.pidm__c = "", NULL, o.pidm__c) AS student_pidm
, CASE WHEN con.wgu_id__c = "" THEN NULL ELSE con.wgu_id__c END AS wgu_id
, to_date(vrm.status_datetime) AS account_created_date
, vrm.status_datetime AS acount_created_datetime
, date_add(vrm.status_datetime, -weekday(vrm.status_datetime) + 6) AS account_created_week
, to_date(last_day(vrm.status_datetime)) AS account_created_month
, vrm.college_code
, vrm.level_code
, vrm.program_code 
, l3.program_group
, IF(vrm.is_sse_student__c = 1, "SSE", "Legacy") AS sse
, INT(vrm.lead_count) AS lead_count
, o.goodbadlead__c AS goodbadlead
, LEFT(o.carestatus__c, 4) AS care_status_latest
, o.outstandingtransferevaluations__c AS outstanding_transfer_evals
, o.transferevaluationstatus__c AS transfer_eval_status
, o.firstevalsent__c AS first_eval_sent
, o.TRANSFEREVALUATIONAPPLICATIONCOMPLETEDT__C AS transfer_eval_app_complete
, o.completedtransferevaluations__c AS completed_transfer_evals
, trim(lower(o.name)) AS name
, trim(lower(o.contactemail__c)) AS email
FROM wgu_analytics.marketing.vw_rst_marketing vrm
LEFT JOIN wgu_lakehouse.salesforce_srm.bi_o_opportunity o ON vrm.opportunity_id = o.id
LEFT JOIN wgu_lakehouse.salesforce_srm.bi_c_contact con ON o.studentcontact__c = con.id
LEFT JOIN level_3 l3 ON l3.program_code = vrm.program_code
WHERE vrm.status_date >= '2023-07-01' 
  AND vrm.care_status_code in ('APIN', 'APNN')
QUALIFY row_number() OVER (
  PARTITION BY vrm.opportunity_id
  ORDER BY lead_count DESC
) = 1;

SELECT * FROM vw_account LIMIT 1000

-- COMMAND ----------

-- DBTITLE 1,Load Function
-- MAGIC %run
-- MAGIC "/Workspace/Users/jonathan.huck@wgu.edu/UEX Reporting/INF/fun_demographics"

-- COMMAND ----------

-- DBTITLE 1,Join Demographics
-- MAGIC %python
-- MAGIC df = join_demographics(table_name = "vw_account", date_col = "account_created_date")
-- MAGIC df.show(10)

-- COMMAND ----------

-- DBTITLE 1,Write
-- MAGIC %python
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("mergeSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_account")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validate

-- COMMAND ----------

DESCRIBE vw_account

-- COMMAND ----------

SELECT 
  count(opportunity_id) AS ids
, count(distinct opportunity_id) AS unique_ids
, abs(count(opportunity_id) - count(distinct opportunity_id)) AS diff
FROM vw_account
WHERE account_created_date < current_date()
--  AND level_code = "UG"
