# Databricks notebook source
# MAGIC %md
# MAGIC # Demographics
# MAGIC Function adds lead demographics to a table.

# COMMAND ----------

def join_demographics(table_name, date_col):
    """
    Joins fields from `dim_lead_demographics` to input table.
    """

    query = f"""
      SELECT 
      t.*
    , d.gender AS gender
    , d.dob
    , datediff(t.{date_col}, to_date(d.dob)) AS age
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
    FROM {table_name} t
    LEFT JOIN wgu_lakehouse.demographics.dim_lead_demographics d
      ON d.opportunity_id = t.opportunity_id
    """
    
    result_df = spark.sql(query)
    return result_df

# COMMAND ----------

# MAGIC %sql
# MAGIC --select
# MAGIC --  student_pidm
# MAGIC --, 
# MAGIC --from wgu_analytics.demographics.vw_student_demographics

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct HOUSEHOLD_INCOME 
# MAGIC --from wgu_analytics.demographics.vw_student_demographics
