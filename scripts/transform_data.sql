-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS SILVER
LOCATION "mnt/datavgsa/silver";

-- COMMAND ----------

CREATE OR REPLACE TABLE SILVER.EVENTS_TYPES AS
SELECT event_type, count(customer_id) number_of_customers
FROM default.silver_usage
GROUP BY event_type;

-- COMMAND ----------

CREATE OR REPLACE TABLE SILVER.CUSTOMER_PLANS AS
SELECT rate_plan_id, count(customer_id) number_of_customers
FROM default.silver_usage
group by rate_plan_id
order by number_of_customers desc;

-- COMMAND ----------

CREATE OR REPLACE TABLE SILVER.USAGE_METRICS AS
SELECT customer_id, sum(duration) usage
FROM default.silver_usage
group by customer_id;
