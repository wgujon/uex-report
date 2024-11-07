-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UEX: Personal Background Questions
-- MAGIC Report applicants' responses to Personal Background questions.

-- COMMAND ----------

-- DBTITLE 1,Inspect
select distinct 
  Question__c as question_text
, count(*) as n
from wgubisalesforce.bi_e_eligibilitystudentanswer
where createddate >= '2024-07-09'
group by 1
order by 2 desc

-- COMMAND ----------

-- DBTITLE 1,Personal Background
create or replace temporary view vw_responses
as
with personal_background as (
  select 
    esa.WGUID__c as wgu_id
  , esa.Student_ID__c as student_id
  , esa.CareProfile__c as care_profile
  , esa.createddate
  , esa.question__c as question_text
  , esa.answer__c as answer_text  
  , case 
      when esa.answer__c = 'no' then 0 
      when esa.answer__c = 'yes' then 1 
      else 1 end as answer_code
  from wgubisalesforce.bi_e_eligibilitystudentanswer esa
  where esa.name != 'Skillset Confidence'
    and createddate >= '2024-07-09'
  -- keep earliest response to each question per WGU ID
  qualify row_number() over (
    partition by wgu_id, question_text
    order by createddate asc
  ) = 1
)

select 
  pb.* 
, a.opportunity_id
, a.college_code
, a.level_code
, a.program_code
, a.sse
, a.app_date
, min(createddate) OVER (partition by wgu_id) as response_time_first
, max(createddate) OVER (partition by wgu_id) as response_time_last
from personal_background pb

-- inner join excludes incomplete apps
join users.jonathan_huck.inf_uex_app a using (wgu_id)

-- keep the earliest application per WGU ID
qualify row_number() over (
  partition by wgu_id
  order by app_date asc
) = 1;

select * from vw_responses;

-- COMMAND ----------

-- DBTITLE 1,Check IDs
with ids as (
  select distinct
    opportunity_id
  , wgu_id
  from vw_responses
)

select
  count(wgu_id)
, count(distinct wgu_id)
, count(opportunity_id)
, count(distinct opportunity_id)
from ids

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pivot

-- COMMAND ----------

-- DBTITLE 1,Mapping
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC raw_df = spark.sql("""select * from vw_responses""")
-- MAGIC
-- MAGIC question_key = {
-- MAGIC     "Are you proficient in English?": "english_proficient",
-- MAGIC     "Are you a salaried employee?": "salaried_employee",
-- MAGIC
-- MAGIC     # Age
-- MAGIC     "Are you at least 14 years of age?": "age14",
-- MAGIC     "Are you 14 years of age or older?": "age14",
-- MAGIC     "Are you at least 16 years of age?": "age16",
-- MAGIC     "Are you at least 18 years of age?": "age18",
-- MAGIC     "Are you 18 years of age or older?": "age18",
-- MAGIC
-- MAGIC     # High school diploma
-- MAGIC     "Did you have a high school diploma or an equivalency?": "hs_diploma",
-- MAGIC     "Do you have a high school diploma or an equivalency?": "hs_diploma",
-- MAGIC     "Do you have a high school diploma or equivalent?": "hs_diploma",
-- MAGIC
-- MAGIC     # RN license
-- MAGIC     "Do you have an RN License?" : "rn_license",
-- MAGIC     "Do you plan to have an RN License prior to completing Enrollment?": "rn_license_plan",
-- MAGIC
-- MAGIC     # Associate's degree or higher
-- MAGIC     "Did you earn an Associates, Bachelor's, or higher Degree?": "assoc_degree_plus",
-- MAGIC     "Did you earn an Associates, Bachelor's, or higher degree?": "assoc_degree_plus",
-- MAGIC     "Did you earn an Associates, Bachelor's or higher degree?": "assoc_degree_plus",
-- MAGIC     "Do you have an Associates, Bachelors or higher degree?": "assoc_degree_plus",
-- MAGIC     "Have you earned an Associates, Bachelors, or higher degree?": "assoc_degree_plus",
-- MAGIC     "Have you earned an Associates, Bachelor's or higher degree?": "assoc_degree_plus",
-- MAGIC
-- MAGIC     # Bachelor's degree or higher
-- MAGIC     "Did you earn a Bachelor's Degree?": "bachelor_degree",
-- MAGIC     "Did you earn a Bachelor's degree?": "bachelor_degree",
-- MAGIC     "Did you earn a Bachelor's Degree or higher?": "bachelor_degree_plus",
-- MAGIC     "Did you earn a Bachelor's, or higher Degree?": "bachelor_degree_plus",
-- MAGIC     "Did you earn a Bachelor's degree or a higher degree?": "bachelor_degree_plus",
-- MAGIC     "Do you have a Bachelor's Degree or higher from an accredited institution?": "bachelor_degree_plus",
-- MAGIC     
-- MAGIC     # Specific Bachelor's degrees
-- MAGIC     "Did you earn a Bachelor's degree or higher in a STEM field?" : "bachelor_stem",
-- MAGIC     "Did you earn a Bachelor's degree or higher in a STEM, Business, or similar quantitative emphasis?" : "bachelor_stem",
-- MAGIC     "Did you earn a Bachelors degree or higher in a STEM, Business, or similar quantitative emphasis" : "bachelor_stem",
-- MAGIC     "Did you earn a Bachelors degree or higher in a STEM field?" : "bachelor_stem",
-- MAGIC     "Did you earn a bachelor's or higher degree in a STEM program?" : "bachelor_stem",
-- MAGIC     "Do you have Bachelors degree or higher in Accounting?" : "bachelor_accounting",
-- MAGIC
-- MAGIC     # Multiple Things (annoying with line breaks)
-- MAGIC     "Do you have a bachelor's degree AND do you satisfy any one of the following criteria?\n- Have you completed previous IT course work at a 300 level or higher?\n- Do you have a current and verifiable industry recognized certification such as; Cisco, CompTIA, AWS, EC-Council, or other industry recognized certification that may transfer in to your program at WGU?\n- Do you have verifiable professional experience, greater than two years, in implementing, managing, or responding to security measures and incidents?" : "bachelor_or_cert_or_2years",
-- MAGIC     "Do you satisfy EITHER of the following criteria?\n - Do you have a current and verifiable industry recognized certification such as; Cisco, CompTIA, AWS, EC-Council, or other industry recognized certification that may transfer in to your program at WGU?\n - Have you completed previous IT course work at a 300 level or higher?": "cert_or_2years",
-- MAGIC     "Do you satisfy all of the following criteria?\n - Did you complete college level coursework with a minimum of 2.75 GPA or higher?\n - Did you successfully complete a pre-calculus, calculus, or higher than calculus math course from an accedited post-secondary academic institution? (earned an Associates or higher)": "bscs_req",
-- MAGIC
-- MAGIC     "Do you satisfy EITHER of the following criteria?\n - Do you have a current and verifiable industry recognized certification such as; Cisco, CompTIA, AWS, EC-Council, or other industry recognized certification that may transfer in to your program at WGU?\n - Have you completed previous IT course work at a 300 level or higher?": "cert_or_2years",
-- MAGIC
-- MAGIC     # Previous experience (not exactly work)
-- MAGIC     "Do you have an active certification in data analytics, data science or data engineering" : "cert_data",
-- MAGIC     "Do you have verifiable, hands-on experience in statistics and computer programming?": "exp_stats_prog",
-- MAGIC
-- MAGIC     # Prerequisites
-- MAGIC     "Did you successfully complete a pre-calculus, calculus, or higher than Calculus math course from an accredited post-secondary academic institution?" : "precalculus",
-- MAGIC
-- MAGIC     # Work Experience
-- MAGIC     "Do you have more than two years of verifiable of work experience in a data analytics, data science, data engineering or database  administration role?" : "work_exp_data",
-- MAGIC     "Do you have verifiable professional experience, greater than two years, in implementing, managing, or responding to security measures and incidents?" : "work_exp_infosec",
-- MAGIC     "Do you have previous work experience involving IT or computer networking?": "work_exp_it",
-- MAGIC     "Do you have previous work experience in cybersecurity?": "work_exp_cyber",
-- MAGIC     "Do you have previous work experience involving computer programming?" : "work_exp_programming",
-- MAGIC     "Do you have more than two years of verifiable, hands-on experience in programming (SQL, Python, R) or statistics?": "work_exp_prog_stat",
-- MAGIC     "Do you have previous work experience involving computer programming or analytics?": "work_exp_prog_stat",
-- MAGIC     "Do you possess a transferable IT Certifications from this list?" : "transfer_it_cert",
-- MAGIC
-- MAGIC     # High school GPA
-- MAGIC     "Did you earn a high school GPA of 2.5 or higher?": "hsgpa25_plus",
-- MAGIC     "Did you graduate from high school with a GPA higher than 2.5?": "hsgpa25_plus",
-- MAGIC     "Did you graduate from high school with a GPA higher than 2.75?" : "hsgpa275_plus",
-- MAGIC     "Did you graduate from high school with a GPA higher than 3.0?" : "hsgpa3_plus",
-- MAGIC     "Do you graduate from high school is a GPA higher than 3.0?" : "hsgpa3_plus",
-- MAGIC     "Did you earn a high school GPA of 3.0 or higher AND a B grade or better in a high school honors, IB, or AP level advanced mathematics course?" : "hsgpa3_with_b_math",
-- MAGIC     "Do you satisfy all of the following criteria?\ n- Do you have a high school diploma or an equivalency?\n- Did you earn a high school GPA of 3.0 or higher? \n- Did you successfully complete a high school honors, IB, or AP level advanced mathematics course with a B or better?": "hsgpa3_hs_diploma_ap_math",
-- MAGIC
-- MAGIC     # College GPA
-- MAGIC     "Was your cumulative college GPA 2.0 or higher?": "college_gpa20_plus",
-- MAGIC     "Was your college cumulative GPA 2.0 or higher?": "college_gpa20_plus",
-- MAGIC     "Was your cumulative college GPA higher than 2.0?": "college_gpa20_plus",
-- MAGIC     "Was your cumulative college GPA higher than 2.5?": "college_gpa25_plus",
-- MAGIC     "Have you attended some college with a cumulative GPA of 2.75 or higher OR Have you never attended college but have a high school diploma or an equivalency with a GPA higher than 3.0?": "gpa275_or_hsgpa30",
-- MAGIC     
-- MAGIC     # GPA (not specified or both)
-- MAGIC     "Was your cumulative GPA higher than 2.5?": "gpa25_plus",
-- MAGIC     "Was your cumulative GPA 2.5 or higher?": "gpa25_plus",
-- MAGIC
-- MAGIC     # Confidence/Studying
-- MAGIC     "Are you able to commit to 20 hours per week to study?": "commit_20hours",
-- MAGIC     "Do you feel comfortable with your current math skills?": "math_skill_comfort",
-- MAGIC
-- MAGIC     # Teaching License
-- MAGIC     "Have you completed a teacher preparation program or alternate route to licensure?": "teacher_prep",
-- MAGIC     "Are you currently a contracted Teacher of Record?": "teacher_of_record",
-- MAGIC     "Do you hold, or have you held, any type of teaching license including restricted, temporary, or emergency?" : "teaching_license"
-- MAGIC }
-- MAGIC
-- MAGIC # create function to map questions to IDs
-- MAGIC def question_map(text):
-- MAGIC     return question_key.get(text, text)
-- MAGIC question_map_udf = udf(question_map, StringType())
-- MAGIC
-- MAGIC # add question IDs to DataFrame
-- MAGIC map_df = raw_df.withColumn("question_id", question_map_udf(raw_df["question_text"]))
-- MAGIC
-- MAGIC display(map_df)

-- COMMAND ----------

-- DBTITLE 1,Write Long
-- MAGIC %python
-- MAGIC #(
-- MAGIC #  map_df.write.format("delta")
-- MAGIC #    .mode("overwrite")
-- MAGIC #    .option("overwriteSchema", True)
-- MAGIC #    .saveAsTable("users.jonathan_huck.inf_uex_personal_background_long")
-- MAGIC #)

-- COMMAND ----------

-- DBTITLE 1,Pivot
-- MAGIC %python
-- MAGIC
-- MAGIC pivot_df = (
-- MAGIC   map_df
-- MAGIC     .groupBy('opportunity_id', 'wgu_id', 'program_code', 'level_code', 'college_code',
-- MAGIC              'response_time_first', 'response_time_last')
-- MAGIC     .pivot('question_id')
-- MAGIC     .agg({'answer_code': 'max'})
-- MAGIC )
-- MAGIC
-- MAGIC pivot_df.createOrReplaceTempView('vw_pivot')
-- MAGIC
-- MAGIC display(pivot_df)

-- COMMAND ----------

describe vw_pivot

-- COMMAND ----------

-- DBTITLE 1,Write
-- MAGIC %python
-- MAGIC
-- MAGIC wide_df = spark.sql(
-- MAGIC   """
-- MAGIC   select 
-- MAGIC     opportunity_id,
-- MAGIC     wgu_id,
-- MAGIC     response_time_first,
-- MAGIC     response_time_last,
-- MAGIC     --age14,
-- MAGIC     --age16,
-- MAGIC     --english_proficient,
-- MAGIC     --age18,
-- MAGIC     hs_diploma as pbq_hs_diploma,
-- MAGIC     assoc_degree_plus as pbq_assoc_degree_plus,
-- MAGIC     --bachelor_accounting,
-- MAGIC     bachelor_degree as pbq_bachelor_degree,
-- MAGIC     bachelor_degree_plus as pbq_bachelor_degree_plus,
-- MAGIC     --bachelor_stem,
-- MAGIC     --cert_data,
-- MAGIC     college_gpa20_plus as pbq_college_gpa20_plus,
-- MAGIC     college_gpa25_plus as pbq_college_gpa25_plus,
-- MAGIC     gpa25_plus as pbq_gpa25_plus,
-- MAGIC     --gpa275_or_hsgpa30,
-- MAGIC     hsgpa25_plus as pbq_hsgpa25_plus
-- MAGIC     --commit_20hours,
-- MAGIC     --math_skill_comfort,
-- MAGIC     --rn_license,
-- MAGIC     --rn_license_plan,
-- MAGIC     --salaried_employee,
-- MAGIC     --teacher_of_record,
-- MAGIC     --teacher_prep,
-- MAGIC     --teaching_license,
-- MAGIC     --work_exp_cyber,
-- MAGIC     --work_exp_data,
-- MAGIC     --work_exp_prog_stat,
-- MAGIC     --work_exp_infosec,
-- MAGIC     --work_exp_it,
-- MAGIC     --work_exp_programming
-- MAGIC   from vw_pivot
-- MAGIC   """)
-- MAGIC
-- MAGIC (
-- MAGIC   wide_df.write.format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", True)
-- MAGIC     .saveAsTable("users.jonathan_huck.inf_uex_personal_background")
-- MAGIC )

-- COMMAND ----------

describe users.jonathan_huck.inf_uex_personal_background
