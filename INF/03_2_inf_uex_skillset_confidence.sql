-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UEX: Skillset Confidence Questions
-- MAGIC This notebook depends on CSV for mapping SCQ responses to On-Ramp recommendations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Long Format
-- MAGIC One record per question per ID; four questions each

-- COMMAND ----------

-- DBTITLE 1,Skillset Confidence
create or replace temporary view vw_skillset_confidence 
as
select distinct
  WGUID__c as wgu_id
, Student_ID__c as student_id

-- convert SRM timestamp to UTC-7
, to_date(from_utc_timestamp(createddate, 'America/Denver')) as createddate
, question__c as question_text
, answer__c as answer_text
, case question__c 
    when "I'm comfortable with advanced math concepts." then "Math"
    when "It is easy for me to combine information from different places and write clear arguments." then "Writing"
    when "I was successful in achieving my goals with my most recent educational experience." then "Resilience"
    when "My long term career goals are clearly defined." then "Career"
    else NULL end as question_code
, case 
    when answer__c in ('strongly_disagree', 'somewhat_disagree') then 1
    else 0 end as flagged
, case answer__c
    when 'strongly_disagree' then 1
    when 'somewhat_disagree' then 2 
    when 'somewhat_agree' then 3
    when 'strongly_agree' then 4
    else NULL end as answer_code
from wgubisalesforce.bi_e_eligibilitystudentanswer
where name = "Skillset Confidence"
;

select * from vw_skillset_confidence
order by createddate desc, wgu_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checks

-- COMMAND ----------

-- DBTITLE 1,Check IDs
select 
  count(wgu_id) / 4
, count(distinct wgu_id) 
from vw_skillset_confidence;

-- COMMAND ----------

-- DBTITLE 1,Duplicates
-- should be 4 questions per ID (any with 8 might mean they responded twice)
select 
  wgu_id
, count(*) as n
, min(createddate) as first_response
, max(createddate) as last_response
, datediff(max(createddate), min(createddate)) as days
from vw_skillset_confidence
group by 1
having count(*) > 4
order by n desc, days desc

-- COMMAND ----------

-- DBTITLE 1,Duplicates per Day
with duplicates as (
  select 
    wgu_id
  , count(*) as n
  , max(createddate) as createddate
  from vw_skillset_confidence
  group by 1
  having count(*) > 4
)

select 
  to_date(createddate) as date
, sum(n) / 8 as ids
from duplicates
group by 1 with rollup
order by 1 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Joins

-- COMMAND ----------

-- DBTITLE 1,TEST
CREATE OR REPLACE TEMPORARY VIEW vw_responses
AS
WITH applications AS (
  SELECT DISTINCT
    opportunity_id
  , wgu_id
  , app_date
  , college_code
  , level_code
  , program_code
  , sse
  FROM users.jonathan_huck.inf_uex_app
  -- SCQ launched on July 9, 2024
  WHERE app_date >= '2024-07-09'
)

select 
  s.*
, a.opportunity_id
, a.college_code
, a.program_code
, a.sse
, a.app_date
-- manually code groups for making recommendations
, case
    when a.college_code = "BU" then "Business"
    when a.program_code IN ('BSHHS', 'BSHIM', 'BSPH', 'BSNT', 'BSHW') THEN "Health Professions"
    when a.program_code IN ('BSPSY') THEN "Psychology"
    when a.program_code IN ('BSHS') THEN "Health Science"
    when a.program_code IN ('BAESELED', 'BAELED', 'BAESSPMM', 'BASPMM',  'BAESSPEE', 'BASPEE') THEN "Elementary-Special Ed"
    when a.program_code IN ("BAESMEMG", 'BSMEMG', 'BAESMES', 'BSMES', 'BAESSEMG', 'BSSEMG', 'BSSESB', 'BAESSESB', 'BAESSESC', 'BSSESC', 'BAESSESE', 'BSSESE', 'BAESSESP', 'BSSESP') THEN "Math-Science Ed"
    when a.program_code IN ('BSCSIA') THEN "BSCIA"
    when a.program_code IN ('BSCS') THEN "BSCS"
    when a.program_code IN ('BSDA') THEN "BSDA"
    when a.program_code LIKE 'BSCC%' THEN "Cloud-IT"
    when a.program_code IN ('BSIT', 'MSITMUG') THEN "Cloud-IT"
    when a.program_code LIKE 'BSNES%' THEN "BSNES"
    when a.program_code LIKE 'BSSWE%' THEN "BSSWE"
    else "Other" end as recommend_code
from vw_skillset_confidence s
join applications a on s.wgu_id = a.wgu_id;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
select 
  count(wgu_id) / 4, 
  count(distinct wgu_id) 
from vw_responses 
;

-- COMMAND ----------

-- DBTITLE 1,Remove Duplicates (TEMPORARY)
-- EdTech needs to fix the problem of responses not being linked to applications
create or replace temporary view vw_responses_dedup
as
select 
  s.*
from vw_responses s
-- get the most recent responses
qualify row_number() over (
  partition by wgu_id, question_code
  order by createddate desc
)=1;

select count(wgu_id) / 4, count(distinct wgu_id) from vw_responses_dedup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Wide Format
-- MAGIC One record per ID

-- COMMAND ----------

-- DBTITLE 1,Pivot
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC response_df = (
-- MAGIC   spark.sql("SELECT * FROM vw_responses_dedup")
-- MAGIC     .groupBy('wgu_id', 'opportunity_id', 'college_code', 'program_code', 'app_date', 'sse', 'recommend_code')
-- MAGIC     .pivot('question_code')
-- MAGIC     .agg({'answer_code': 'max'})
-- MAGIC ).toPandas()
-- MAGIC
-- MAGIC # create binary flags for answering strongly or somewhat disagree
-- MAGIC response_df['SC1'] = response_df['Math'].apply(lambda x: 1 if x <= 2 else 0)
-- MAGIC response_df['SC2'] = response_df['Writing'].apply(lambda x: 1 if x <= 2 else 0)
-- MAGIC response_df['SC3'] = response_df['Resilience'].apply(lambda x: 1 if x <= 2 else 0)
-- MAGIC response_df['SC4'] = response_df['Career'].apply(lambda x: 1 if x <= 2 else 0)
-- MAGIC
-- MAGIC display(response_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Course Mapping

-- COMMAND ----------

-- DBTITLE 1,Import Mapping
-- MAGIC %python
-- MAGIC
-- MAGIC # EdTech's CSV for mapping all combinations of recommendations
-- MAGIC csv_df = pd.read_csv('/Volumes/users/jonathan_huck/trailheads/Recommendation_Logic_240819.csv')
-- MAGIC
-- MAGIC mapping_df = csv_df.pivot_table(
-- MAGIC     index=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'],
-- MAGIC     columns='recommendation sequence',
-- MAGIC     values='Product',
-- MAGIC     aggfunc='first'
-- MAGIC ).reset_index()
-- MAGIC
-- MAGIC mapping_df.columns = ['program_code', 'SC1', 'SC2', 'SC3', 'SC4',
-- MAGIC                       'Product1', 'Product2', 'Product3']
-- MAGIC
-- MAGIC display(mapping_df.sort_values(by=['program_code', 'SC1', 'SC2', 'SC3', 'SC4']))

-- COMMAND ----------

-- DBTITLE 1,Cleaning
-- MAGIC %python
-- MAGIC # Strip whitespace from object columns
-- MAGIC response_df['program_code'] = response_df['program_code'].str.strip()
-- MAGIC mapping_df['program_code'] = mapping_df['program_code'].str.strip()
-- MAGIC
-- MAGIC # Ensure that all other relevant columns are also stripped and properly formatted
-- MAGIC for col in ['SC1', 'SC2', 'SC3', 'SC4']:
-- MAGIC     response_df[col] = response_df[col].astype(str).str.strip().astype(int)
-- MAGIC     mapping_df[col] = mapping_df[col].astype(str).str.strip().astype(int)

-- COMMAND ----------

-- DBTITLE 1,Checking
-- MAGIC %python
-- MAGIC # Check for duplicates in mapping_df
-- MAGIC duplicates = mapping_df.duplicated(subset=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'], keep=False)
-- MAGIC if duplicates.any():
-- MAGIC     print("Duplicates:")
-- MAGIC     print(mapping_df[duplicates])
-- MAGIC
-- MAGIC comparison_df = pd.merge(response_df, mapping_df, on=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'], how='outer', indicator=True)
-- MAGIC
-- MAGIC # Check rows that didn't match
-- MAGIC mismatch_df = comparison_df[comparison_df['_merge'] != 'both']
-- MAGIC display(mismatch_df)

-- COMMAND ----------

-- DBTITLE 1,Apply Mapping
-- MAGIC %python
-- MAGIC
-- MAGIC merged_df = pd.merge(response_df, mapping_df, 
-- MAGIC                      on=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'], how='left')
-- MAGIC
-- MAGIC display(merged_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC duplicates = mapping_df.duplicated(subset=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'], keep=False)
-- MAGIC if duplicates.any():
-- MAGIC     print("Duplicates found:")
-- MAGIC     print(mapping_df[duplicates])

-- COMMAND ----------

-- DBTITLE 1,Check missing mappings
-- MAGIC %python 
-- MAGIC missing_mappings = response_df.merge(mapping_df, on=['program_code', 'SC1', 'SC2', 'SC3', 'SC4'], how='left', indicator=True)
-- MAGIC missing_mappings = missing_mappings[missing_mappings['_merge'] == 'left_only']
-- MAGIC display(missing_mappings)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Calculate the percentage of each outcome
-- MAGIC #outcome_counts = merged_df["Product1"].value_counts(normalize=True) * 100
-- MAGIC outcome_counts = merged_df.groupby('program_code')['Product1'].value_counts(normalize=True) * 100
-- MAGIC
-- MAGIC display(outcome_counts)

-- COMMAND ----------

-- DBTITLE 1,Inspect Table
-- MAGIC %python
-- MAGIC display(merged_df)

-- COMMAND ----------

-- DBTITLE 1,Write Wide
-- MAGIC %python
-- MAGIC wide_df = spark.createDataFrame(merged_df)
-- MAGIC
-- MAGIC (
-- MAGIC   wide_df.write.format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", True)
-- MAGIC     .saveAsTable("users.jonathan_huck.inf_uex_skill_conf")
-- MAGIC )
