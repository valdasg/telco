-- Databricks notebook source
SELECT event_type, count(customer_id) number_of_customers
FROM default.silver_usage
group by event_type;


-- COMMAND ----------

SELECT rate_plan_id, count(customer_id) number_of_customers
FROM default.silver_usage
group by rate_plan_id
order by number_of_customers desc;

-- COMMAND ----------

SELECT customer_id, sum(duration) usage
FROM default.silver_usage
group by customer_id;

-- COMMAND ----------

message = 'Your visualisations are ready. View them on your Databrics Account / Dashboards'
email_logs(message)

-- COMMAND ----------

dbutils.notebook.exit('Success')
