-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Trailheads: On-Ramp and Certificate Starts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Academy Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### New Starts

-- COMMAND ----------

--select * from wgu_analytics.academics_academy.rst_academy_student_week limit 10

-- COMMAND ----------

create or replace temporary view vw_start
as
select 
  wgu_id 
, contact_id
, s.subscription_id
, new_start as acad_start_flag
, to_date(month_end_date) as acad_start_month
, academy_stage_date as acad_start_date
, expiration_date
, s.referral_channel__c as referral_channel
, case 
    when s.offering_type in ('Single Courses', 'Course Bundles') then 'On-Ramp'
    when s.offering_type = 'Microcredentials' then 'Certificate'
    end as offering_type
, case s.offering_type
    when 'Single Courses' then 'Single Course'
    when 'Course Bundles' then 'Course Bundle'
    when 'Microcredentials' then 'Certificate'
    else 'Missing' end as offering_subtype
, ifnull(offering_name, 'Missing') as product_code
, s.title as product_name
, ifnull(if(s.client_name__c = 'null', NULL, s.client_name__c), "None") as partnership
, case when c.client_subclass__c like '%Trailheads%' THEN 'Trailheads' else NULL end as client_subclass

-- Quality assurance check: some records are missing start date
-- can I use the expiration date to estimate the missing start date? not much consistency it seems
--, case when academy_stage_date is null then 1 else 0 end qa_impute_start_date

from wgu_analytics.academics_academy.rst_academy_student s
left join hive_metastore.wguacademy.contacts as c on c.id = s.contact_id
where new_start = 1
;

select * from vw_start limit 100;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
-- Duplicate IDs because students have multiple subscriptions
SELECT
  client_subclass
, count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
FROM vw_start
GROUP BY 1 WITH ROLLUP

-- COMMAND ----------

-- DBTITLE 1,Compare MBR from IR
with etl as (
  select
    last_day(acad_start_month) as start_month
  , sum(acad_start_flag) as n_etl
  from vw_start
  group by 1
),

mbr as (
  select
    to_date(end_date) as start_month
  , sum(enrolled) as n_mbr 
  from wgu_analytics.academics_academy.dbv_wbr_acad_enrollment
  where time_period = 'Monthly'
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

select
    last_day(acad_start_month) as start_month
, sum(acad_start_flag) as n_etl
from vw_start
where client_subclass = 'Trailheads'
group by 1

-- COMMAND ----------

-- DBTITLE 1,Compare MBR from Academy
-- MAGIC %md
-- MAGIC -- HOW CAN I ACCESS THIS ON UNITY CATALOG
-- MAGIC WITH mbr AS (
-- MAGIC   SELECT 
-- MAGIC     last_day(reportdate) as start_month
-- MAGIC   , sum(denominator) as n_mbr
-- MAGIC   FROM (
-- MAGIC     SELECT 
-- MAGIC       wbr.contact_id
-- MAGIC     , wbr.denominator
-- MAGIC     , wbr.reportdate
-- MAGIC     FROM wguacademy.tvz_wbr_compendium_tempv3 wbr
-- MAGIC     WHERE wbr.denominator <> 0
-- MAGIC       AND wbr.index IS NOT NULL
-- MAGIC       AND wbr.client IS NULL -- exclude clients like Amazon, etc.
-- MAGIC      AND wbr.metric = "6_newstarts"
-- MAGIC   )
-- MAGIC ),
-- MAGIC
-- MAGIC etl as (
-- MAGIC   select
-- MAGIC     last_day(acad_start_date) as start_month
-- MAGIC   , sum(acad_start_flag) as n_etl
-- MAGIC   from vw_start
-- MAGIC   group by 1
-- MAGIC )
-- MAGIC
-- MAGIC select 
-- MAGIC   etl.start_month
-- MAGIC , n_etl
-- MAGIC , n_mbr
-- MAGIC , n_etl - n_mbr as diff
-- MAGIC , round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
-- MAGIC from etl
-- MAGIC join mbr using (start_month)
-- MAGIC order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Completion

-- COMMAND ----------

create or replace temporary view vw_complete
as
select 
  wgu_id 
, contact_id
, subscription_id
, offering_complete as acad_complete_flag
, month_end_date as acad_complete_date
, to_date(academy_stage_date) as acad_complete_month
--, expiration_date -- do I need to compare expiration date?
, s.referral_channel__c as referral_channel
, ifnull(offering_type, 'Unavailable') as offering_type
, ifnull(offering_name, 'Unavailable') as offering_name
, s.title as product
--, ifnull(if(s.client_name__c = 'null', NULL, s.client_name__c), "None") as partnership
from wgu_analytics.academics_academy.rst_academy_student s
where offering_complete = 1
;

select * from vw_complete limit 100;

-- COMMAND ----------

-- DBTITLE 1,Unique IDs
SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
FROM vw_complete

-- COMMAND ----------

-- DBTITLE 1,Compare MBR from IR
with etl as (
  select
    last_day(acad_complete_date) as report_month
  , sum(acad_complete_flag) as n_etl
  from vw_complete
  group by 1
),

mbr as (
  select
    to_date(end_date) as report_month
  , sum(completions) as n_mbr 
  from wgu_analytics.academics_academy.dbv_wbr_acad_completions
  where time_period = 'Monthly'
  group by 1
)

select 
  etl.report_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
from etl
join mbr using (report_month)
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,Compare MBR from Academy
-- MAGIC %md
-- MAGIC WITH mbr AS (
-- MAGIC   SELECT 
-- MAGIC     last_day(reportdate) as start_month
-- MAGIC   , sum(denominator) as n_mbr
-- MAGIC   FROM (
-- MAGIC     SELECT 
-- MAGIC       wbr.contact_id
-- MAGIC     , wbr.denominator
-- MAGIC     , wbr.reportdate
-- MAGIC     FROM wguacademy.tvz_wbr_compendium_tempv3 wbr
-- MAGIC     WHERE wbr.denominator <> 0
-- MAGIC       AND wbr.index IS NOT NULL
-- MAGIC       AND wbr.client IS NULL -- exclude clients like Amazon, etc.
-- MAGIC      AND wbr.metric = "6_newstarts"
-- MAGIC   )
-- MAGIC ),
-- MAGIC
-- MAGIC etl as (
-- MAGIC   select
-- MAGIC     last_day(acad_start_date) as start_month
-- MAGIC   , sum(acad_start_flag) as n_etl
-- MAGIC   from vw_start
-- MAGIC   group by 1
-- MAGIC )
-- MAGIC
-- MAGIC select 
-- MAGIC   etl.start_month
-- MAGIC , n_etl
-- MAGIC , n_mbr
-- MAGIC , n_etl - n_mbr as diff
-- MAGIC , round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
-- MAGIC from etl
-- MAGIC join mbr using (start_month)
-- MAGIC order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drops

-- COMMAND ----------

-- DBTITLE 1,Drop
create or replace temporary view vw_drop
as
select 
  wgu_id 
, contact_id
, subscription_id
, case 
    when drops = 1 then 1 
    when 7d_cancellation = 1 then 1
    when 72hr_Cancellation = 1 then 1
    else 0 end as acad_drop_flag

-- distinguish type of drop
, if(7d_cancellation = 1, 1, 0) as acad_drop_7d_flag
, if(72hr_cancellation = 1, 1, 0) as acad_drop_72hr_flag

, academy_stage_date as acad_drop_date
, to_date(month_end_date) as acad_drop_month
, s.referral_channel__c as referral_channel
, ifnull(offering_type, 'Unavailable') as offering_type
, ifnull(offering_name, 'Unavailable') as offering_name
, s.title as product
, ifnull(if(s.client_name__c = 'null', NULL, s.client_name__c), "None") as partnership
from wgu_analytics.academics_academy.rst_academy_student s
where case 
        when drops = 1 then 1 
        when 7d_cancellation = 1 then 1
        when 72hr_Cancellation = 1 then 1
        else 0 end = 1
;

select * from vw_drop limit 100;

-- COMMAND ----------

-- DBTITLE 1,Unique IDs
SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
FROM vw_drop

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Joins

-- COMMAND ----------

-- DBTITLE 1,Academy
create or replace temporary view vw_academy
as
with new_starts as (
  -- query the earliest start record
  select * from vw_start
  qualify row_number() over (
    partition by wgu_id, contact_id
    order by acad_start_date asc
  ) = 1
),

completers as (
  select 
    s.wgu_id
  , c.acad_complete_flag
  , c.acad_complete_date
  from vw_start s
  join vw_complete c 
    on s.wgu_id = c.wgu_id
    and s.acad_start_date <= c.acad_complete_date
  qualify row_number() over (
    partition by s.wgu_id
    order by acad_complete_date asc
  ) = 1
),

drops as (
  select 
    s.wgu_id
  , acad_drop_flag
  , acad_drop_date
  , acad_drop_7d_flag
  , acad_drop_72hr_flag
  from vw_start s
  join vw_drop d 
    on s.wgu_id = d.wgu_id
    and s.acad_start_date <= d.acad_drop_date
  qualify row_number() over (
    partition by s.wgu_id
    order by acad_drop_date asc
  ) = 1
)

select 
  s.*
, ifnull(c.acad_complete_flag, 0) as acad_complete_flag
, c.acad_complete_date
, ifnull(d.acad_drop_flag, 0) as acad_drop_flag
, ifnull(d.acad_drop_7d_flag, 0) as acad_drop_7d_flag
, ifnull(d.acad_drop_72hr_flag, 0) as acad_drop_72hr_flag
, d.acad_drop_date
from vw_start s
left join completers c on s.wgu_id = c.wgu_id
left join drops d on s.wgu_id = d.wgu_id
;

select * from vw_academy

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation

-- COMMAND ----------

with etl as (
  select
    last_day(acad_start_date) as start_month
  , count(contact_id) as n_etl
  from vw_academy
  group by 1
),

mbr as (
  select
    to_date(end_date) as start_month
  , sum(enrolled) as n_mbr --initial_term_enrollments?
  from wgu_analytics.academics_academy.dbv_wbr_acad_enrollment
  where time_period = 'Monthly'
    --and partnership = 'WGU'
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ID Matching

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read \
-- MAGIC   .format("delta") \
-- MAGIC   .load("/FileStore/jonathanhuck/stg_match_wgu_academy")
-- MAGIC  
-- MAGIC df.createOrReplaceTempView("stg_match_wgu_academy")

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_match
AS
SELECT 
  a.*,
  b.opportunity_id,
  b.pidm__c AS student_pidm,
  CASE 
    WHEN b.Opportunity_ID IS NULL THEN 'Unmatched Opportunity' 
    ELSE b.Opportunity_ID END AS Referal_Opportunity,
  b.FAA_Date,
  b.WGU_Enrolled_Program,
  b.WGU_Status
FROM vw_academy a
LEFT JOIN stg_match_wgu_academy b 
  ON a.contact_id = b.Acad_Contact_ID 
  --AND b.MinMatchReferSeq = TRUE
  AND MinMatchAcadEnrollSeq = TRUE
;

-- COMMAND ----------

-- DBTITLE 1,Write
-- MAGIC %python
-- MAGIC df = spark.sql("select * from vw_match")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_academy")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation

-- COMMAND ----------

describe users.jonathan_huck.inf_uex_academy

-- COMMAND ----------

SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM users.jonathan_huck.inf_uex_academy
WHERE client_subclass = "Trailheads"


-- COMMAND ----------

with etl as (
  select
    last_day(acad_start_date) as start_month
  , count(opportunity_id) as n_etl
  from users.jonathan_huck.inf_uex_academy
  group by 1
),

mbr as (
  select
    to_date(end_date) as start_month
  , sum(enrolled) as n_mbr --initial_term_enrollments?
  from wgu_analytics.academics_academy.dbv_wbr_acad_enrollment
  where time_period = 'Monthly'
    --and partnership = 'WGU'
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr * 100, 0) as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

select
    last_day(acad_start_date) as start_month
  , count(opportunity_id) as n_etl
from users.jonathan_huck.inf_uex_academy
where client_subclass = 'Trailheads'
group by 1
order by 1

-- COMMAND ----------

SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM users.jonathan_huck.inf_uex_academy
where acad_start_date is not null

-- COMMAND ----------

WITH starts AS (
SELECT *
FROM wgubidev.tvz_trailheads_referral_cohort
where acad_enrollment_date is not null
QUALIFY row_number() OVER (
    PARTITION BY opportunity_id
    ORDER BY acad_enrollment_date ASC
  ) = 1
)

SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM starts

-- COMMAND ----------

WITH starts AS (
SELECT * FROM users.jonathan_huck.inf_uex_academy
WHERE client_subclass = "Trailheads"
QUALIFY row_number() OVER (
    PARTITION BY opportunity_id
    ORDER BY acad_start_date ASC
  ) = 1
)

SELECT
  count(wgu_id) AS wgu
, count(distinct wgu_id) AS unique_wgu
, count(contact_id) AS contact
, count(distinct contact_id) AS unique_contact
, count(opportunity_id) AS oppo
, count(distinct opportunity_id) AS unique_oppo
FROM starts

-- COMMAND ----------

SELECT * FROM users.jonathan_huck.inf_uex_academy
WHERE client_subclass = "Trailheads"
QUALIFY row_number() OVER (
    PARTITION BY opportunity_id
    ORDER BY acad_start_date ASC
  ) = 1
