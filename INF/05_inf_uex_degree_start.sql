-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UEX: Degree New Starts
-- MAGIC One record per new start including flag with 45-day attrition.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Degree New Starts

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_new_start 
AS

WITH level_3 AS (
  SELECT DISTINCT
    level_3 AS program_group
  , program_code
  FROM wgu_analytics.executive_report.vw_de_finance_program_grouping
  WHERE report_date = (
    SELECT MAX(report_date) 
    FROM wgu_analytics.executive_report.vw_de_finance_program_grouping
  )
)

SELECT 
  to_date(ns.closedate) AS start_date
, to_date(last_day(ns.closedate)) AS start_month
, INT(o.pidm__c) AS student_pidm
, ns.opportunityid AS opportunity_id
, ns.college_code
, CASE
    WHEN ns.program_code NOT LIKE 'M%' OR ns.program_code LIKE 'MSITMUG' THEN 'UG'
    ELSE 'GR' END AS level_code
, l3.program_group
, ns.program_code
, ns.wgu_region AS region
, ns.ethnicity_reported AS ethnicity
, ns.person_of_color AS bhi2
, CASE 
    WHEN ns.household_income_level IN ("High", "Low") THEN ns.household_income_level
    ELSE "Missing" END AS household_income_level
, d.household_income
, ns.academy_stage__c as academy_stage
, CASE 
    WHEN ns.academy_stage__c = "Graduated" THEN 1
    ELSE 0 END AS academy_completed_flag
, CASE 
    WHEN ns.academy_stage__c != "Never at Academy" AND ns.academy_stage__c IS NOT NULL THEN 1
    ELSE 0 END AS academy_attended_flag
, d.gender
, d.employment_status
FROM wgu_lakehouse.enrollment.vw_inf_leads_newstart ns
LEFT JOIN wgu_lakehouse.salesforce_srm.bi_o_opportunity o ON o.id = ns.opportunityid
LEFT JOIN wgu_analytics.demographics.vw_student_demographics d ON d.student_pidm = o.pidm__c 
LEFT JOIN level_3 l3 USING (program_code)
WHERE ns.closedate >= '2020-07-01';

SELECT * FROM vw_new_start LIMIT 100;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
SELECT 
  count(opportunity_id)
, count(distinct opportunity_id) 
FROM vw_new_start

-- COMMAND ----------

-- DBTITLE 1,MBR
with etl as (
  select
    last_day(start_date) as start_month
  , count(opportunity_id) as n_etl
  from vw_new_start
  group by 1
),

mbr as (
  select
    month_end_date as start_month
  , int(sum(newstarts)) as n_mbr
  from wgubi.dbv_exe_newstart_college
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Day 45 Attrition

-- COMMAND ----------

-- this code is modified from `tvz_45_attrition_v2` to get ID-level data
-- rough opportunity ID matching is needed for reverse TSCA
-- query returns multiple records per ID per week of term
create or replace temporary view vw_attrition 
as
with rst_student_tsca as (
  select 
    t.MONTH_END_DATE
  , t.MONTH_BEGIN_DATE
  , t.STUDENT_ID
  , int(t.STUDENT_PIDM) as student_pidm
  , t.TERM_START_DATE
  , t.TERM_END_DATE
  , t.COLLEGE_CODE
  , t.PROGRAM_CODE
  , t.REVERSE_ACTIVE_TSCA
  from wgubi.vw_rst_student t
  left join (
    select * 
    from wgubi.fact_stu_45day_attrition 
    where upper(Attrition_45Day_Flag_Status) not in ('TSCA', 'ACTIVE') 
      and wk_of_term = 8 
    ) s 
    on s.student_pidm = t.student_pidm 
    and s.term_start_date = t.term_start_date
  where t.REVERSE_ACTIVE_TSCA = 1
    and s.student_pidm is null
),

-- get opportunity ID for TSCA records
oppo_tsca as (
  select
    a.*
  , b.start_date
  , b.opportunity_id
  from rst_student_tsca a
  left join vw_new_start b
    on a.student_pidm = b.student_pidm
    and a.term_start_date = b.start_date
  qualify row_number() over (
    partition by a.student_pidm, a.term_start_date
    order by a.term_start_date
  ) = 1
)

SELECT 
  sa.opportunityid as opportunity_id
, int(sa.student_pidm) as student_pidm
, to_date(sa.term_start_date) as term_start_date
, sa.wk_of_term
, to_date(sa.term_wk_end) as term_wk_end
, sa.college_code
, sa.program_code
, sa.Attrition_45Day_Flag_Status
, sa.Attrition_45day_flag
FROM wgubi.fact_stu_45day_attrition sa
where to_date(sa.term_start_date) >= '2020-07-01'
  and sa.wk_of_term <= 8
  and upper(sa.Attrition_45Day_Flag_Status) <> 'TSCA' 

UNION ALL

select 
  rs.opportunity_id
, int(rs.student_pidm) as student_pidm
, to_date(rs.TERM_START_DATE) as term_start_date
, wk.wk_of_term
, to_date(wk.TERM_WK_END) as term_wk_end
, rs.COLLEGE_CODE
, rs.PROGRAM_CODE
, 'TSCA' as Attrition_45Day_Flag_Status
, rs.REVERSE_ACTIVE_TSCA AS Attrition_45day_flag
from oppo_tsca rs
join (
  select DISTINCT TERM_START_DATE, TERM_WK_END, WK_OF_TERM 
  from wgubi.DBT_EXE_TERM_WEEK where WK_OF_TERM = 8
) wk 
on wk.TERM_START_DATE = rs.term_start_date
where rs.term_start_date >= '2020-07-01'
  and wk.TERM_WK_END <= current_date();

select * from vw_attrition limit 1000

-- COMMAND ----------

-- DBTITLE 1,Check IDs
-- lots of duplicates to be aware of here
SELECT 
  attrition_45day_flag
, count(student_pidm)
, count(distinct student_pidm)
, count(opportunity_id)
, count(distinct opportunity_id) 
FROM vw_attrition
GROUP BY 1 WITH ROLLUP
ORDER BY 1

-- COMMAND ----------

-- DBTITLE 1,Check IDs + Terms
-- Some IDs have records for multiple terms
SELECT 
  count(concat(student_pidm, term_start_date)) as pidm_terms
, count(distinct concat(student_pidm, term_start_date)) unique_pidm_terms
, count(concat(opportunity_id, term_start_date)) as oppo_terms
, count(distinct concat(opportunity_id, term_start_date)) as unique_oppo_terms
FROM vw_attrition
where attrition_45day_flag = 1

-- COMMAND ----------

-- DBTITLE 1,MBR
with etl as (
  select
    last_day(term_start_date) as report_month
  , count(opportunity_id)  as n_etl
  , count(distinct opportunity_id) as unique_n_etl
  from vw_attrition
  where Attrition_45day_flag = 1
  group by 1
),

mbr as (
  select
    last_day(term_start_date) as report_month
  , int(sum(N_attrition)) as n_mbr
  from wgu_analytics.executive_report.dbt_exe_45d_attrition
  group by 1
  order by 1 desc 
)

select 
  etl.report_month
, n_etl
, n_mbr
, n_etl - n_mbr as n_diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as n_perc_diff
, unique_n_etl
from etl
join mbr using (report_month)
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,Unique IDs
create or replace temporary view vw_attrition_unique 
as
select
  opportunity_id
, student_pidm
, term_start_date
, term_wk_end
, college_code
, program_code
, attrition_45day_flag
, attrition_45day_flag_status
from vw_attrition
where attrition_45day_flag = 1
  and opportunity_id is not null -- screen a couple missing
qualify row_number() over (
  partition by opportunity_id, student_pidm, term_start_date
  order by term_wk_end desc
) = 1;

SELECT 
  count(student_pidm) as pidm
, count(distinct student_pidm) as unique_pidm
, count(opportunity_id) as oppo
, count(distinct opportunity_id) as unique_oppo
FROM vw_attrition_unique;

-- COMMAND ----------

-- DBTITLE 1,Compare
-- does this match unique records in original query?
with etl as (
  select
    last_day(term_start_date) as report_month
  , count(opportunity_id) as n_etl
  from vw_attrition_unique
  group by 1
),

original as (
  select
    last_day(term_start_date) as report_month
  , count(distinct opportunity_id)  as n_unique
  from vw_attrition
  where Attrition_45day_flag = 1
  group by 1
)

select 
  etl.report_month
, n_etl
, n_unique
, n_etl - n_unique as n_diff
, round((n_etl - n_unique) / n_unique, 3) * 100 as n_perc_diff
from etl
join original using (report_month)
order by 1 desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Join

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_export
AS
WITH day45 AS (
  SELECT b.*
  FROM vw_new_start a
  JOIN vw_attrition_unique b 
    ON a.opportunity_id = b.opportunity_id 
    AND a.start_date = b.term_start_date
  QUALIFY row_number() OVER (
    PARTITION BY b.opportunity_id
    ORDER BY term_start_date
  ) = 1
)

SELECT
  a.*
, b.attrition_45day_flag
, b.attrition_45day_flag_status AS attrition_45day_status
FROM vw_new_start a
LEFT JOIN vw_attrition_unique b 
  ON a.opportunity_id = b.opportunity_id 
  AND a.start_date = b.term_start_date
;

-- check for the same new starts
SELECT
  'Before Join' AS label
, count(opportunity_id) as oppo
, count(distinct opportunity_id) as unique_oppo
FROM vw_new_start
GROUP BY 1
UNION ALL
SELECT
  'After Join' AS label
, count(opportunity_id) as oppo
, count(distinct opportunity_id) as unique_oppo
FROM vw_export
GROUP BY 1
;

-- COMMAND ----------

-- DBTITLE 1,Compare
-- should also match Hassan's dashboard
-- https://bitableau.wgu.edu/#/views/45_Day_Attrition_Student_List
with etl as (
  select
    last_day(start_date) as report_month
  , count(opportunity_id) as n_etl
  from vw_export
  where Attrition_45day_flag = 1
  group by 1
),

original as (
  select
    last_day(term_start_date) as report_month
  , count(distinct opportunity_id)  as n_unique
  from vw_attrition
  where Attrition_45day_flag = 1
  group by 1
)

select 
  etl.report_month
, n_etl
, n_unique
, n_etl - n_unique as n_diff
, round((n_etl - n_unique) / n_unique, 3) * 100 as n_perc_diff
from etl
join original using (report_month)
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Write

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("""select * from vw_export""")
-- MAGIC
-- MAGIC (
-- MAGIC   df.write.format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .saveAsTable("users.jonathan_huck.inf_uex_degree_start")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation

-- COMMAND ----------

describe users.jonathan_huck.inf_uex_degree_start

-- COMMAND ----------

select * from users.jonathan_huck.inf_uex_degree_start limit 100

-- COMMAND ----------

-- DBTITLE 1,MBR - Degree New Starts
with etl as (
  select
    last_day(start_date) as start_month
  , count(opportunity_id) as n_etl
  from users.jonathan_huck.inf_uex_degree_start
  group by 1
),

mbr as (
  select
    month_end_date as start_month
  , int(sum(newstarts)) as n_mbr
  from wgubi.dbv_exe_newstart_college
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

-- DBTITLE 1,MBR - Academy Completers
with etl as (
  select
    last_day(start_date) as start_month
  , count(opportunity_id) as n_etl
  from users.jonathan_huck.inf_uex_degree_start
  where academy_completed_flag = 1
  group by 1
),

mbr as (
  select
    month_end_date as start_month
  , int(sum(newstarts)) as n_mbr
  from wgubi.dbv_exe_newstart_college
  where academy_stage = 'Graduated'
  group by 1
)

select 
  etl.start_month
, n_etl
, n_mbr
, n_etl - n_mbr as diff
, round((n_etl - n_mbr) / n_mbr, 3) * 100 as perc_diff
from etl
join mbr using (start_month)
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Notes

-- COMMAND ----------

-- DBTITLE 1,PROD query for day 45 attrition
-- MAGIC %md
-- MAGIC --https://wgu-prod.cloud.databricks.com/editor/notebooks/2084782337700357
-- MAGIC create or replace temporary view vw_attrition
-- MAGIC AS
-- MAGIC WITH rst_student_tsca AS (
-- MAGIC   select 
-- MAGIC     t.MONTH_END_DATE
-- MAGIC   , t.MONTH_BEGIN_DATE
-- MAGIC   , t.STUDENT_ID
-- MAGIC   , t.STUDENT_PIDM
-- MAGIC   , t.TERM_START_DATE
-- MAGIC   , t.TERM_END_DATE
-- MAGIC   , t.TERM_CODE
-- MAGIC   , t.TERM_CODE_CTLG
-- MAGIC   , t.COLLEGE_CODE
-- MAGIC   , t.PROGRAM_CODE
-- MAGIC   , t.REVERSE_ACTIVE_TSCA
-- MAGIC   from wgubi.vw_rst_student t
-- MAGIC   left join (
-- MAGIC     select * 
-- MAGIC     from wgubi.fact_stu_45day_attrition 
-- MAGIC     where upper(Attrition_45Day_Flag_Status) not in ('TSCA', 'ACTIVE') 
-- MAGIC       and wk_of_term = 8 
-- MAGIC     ) s 
-- MAGIC     on s.student_pidm = t.student_pidm and s.term_start_date = t.term_start_date --remove if tsca dups
-- MAGIC   where t.REVERSE_ACTIVE_TSCA = 1
-- MAGIC     and s.student_pidm is null
-- MAGIC )
-- MAGIC
-- MAGIC SELECT 
-- MAGIC   to_date(sa.term_start_date) as term_start_date
-- MAGIC , sa.wk_of_term
-- MAGIC , to_date(sa.term_wk_end) as term_wk_end
-- MAGIC , sa.college_code
-- MAGIC , sa.program_code
-- MAGIC , sa.Attrition_45Day_Flag_Status
-- MAGIC , sum(sa.Attrition_45day_flag) as N_attrition
-- MAGIC , count(sa.Attrition_45day_flag) as N_attrition_denom
-- MAGIC FROM wgubi.fact_stu_45day_attrition sa
-- MAGIC where to_date(sa.term_start_date) >= '2019-07-01'
-- MAGIC and sa.wk_of_term <=8
-- MAGIC and upper(sa.Attrition_45Day_Flag_Status) <> 'TSCA' 
-- MAGIC group by all
-- MAGIC
-- MAGIC UNION ALL
-- MAGIC
-- MAGIC select 
-- MAGIC   to_date(rs.TERM_START_DATE) as term_start_date
-- MAGIC , wk.wk_of_term
-- MAGIC , to_date(wk.TERM_WK_END) as term_wk_end
-- MAGIC , rs.COLLEGE_CODE
-- MAGIC , rs.PROGRAM_CODE
-- MAGIC , 'TSCA' as Attrition_45Day_Flag_Status
-- MAGIC , sum(rs.REVERSE_ACTIVE_TSCA) as N_attrition
-- MAGIC , count(rs.REVERSE_ACTIVE_TSCA) as N_attrition_denom
-- MAGIC from rst_student_tsca rs
-- MAGIC JOIN (select DISTINCT TERM_START_DATE, TERM_WK_END, WK_OF_TERM 
-- MAGIC from wgubi.DBT_EXE_TERM_WEEK where WK_OF_TERM = 8) wk 
-- MAGIC on wk.TERM_START_DATE = rs.term_start_date
-- MAGIC
-- MAGIC where rs.term_start_date >= '2019-07-01'
-- MAGIC   and wk.TERM_WK_END <= current_date() -- TSCA will only show up after week 8
-- MAGIC group by all
