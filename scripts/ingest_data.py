# Databricks notebook source
dbutils.widgets.text('data_source', 'upload')
data_source = dbutils.widgets.get('data_source')

# COMMAND ----------

# MAGIC %run "../constants/constants"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, col

# COMMAND ----------

telco_schema = StructType(fields = [
    StructField('customer_ID', IntegerType(), nullable=False),
    StructField('Event Start Time', StringType(), nullable=True),
    StructField('Event Type', StringType(), nullable=True),
    StructField('Rate Plan ID', StringType(), nullable=True),
    StructField('Billing Flag 1', StringType(), nullable=True),
    StructField('Billing Flag 2', StringType(), nullable=True),
    StructField('Duration', IntegerType(), nullable=True),
    StructField('Charge', DoubleType(), nullable=True),
    StructField('Month', StringType(), nullable=True)
])

# COMMAND ----------


telco_df = spark.read \
    .option('header', 'true') \
    .schema(telco_schema) \
    .csv(f'dbfs:/mnt/{storage_account_name}/raw/{file_date}/usage.csv')



# COMMAND ----------

telco_df = telco_df \
    .withColumnRenamed('customer_ID', 'customer_id') \
    .withColumn('event_start_time', col('Event Start Time').cast("timestamp")) \
    .withColumnRenamed('Event Type', 'event_type') \
    .withColumnRenamed('Rate Plan ID', 'rate_plan_id') \
    .withColumnRenamed('Billing Flag 1', 'billing_flag_1') \
    .withColumnRenamed('Billing Flag 2', 'billing_flag_2') \
    .withColumnRenamed('Duration', 'duration') \
    .withColumnRenamed('Charge', 'charge') \
    .withColumnRenamed('Month', 'month') \
    .withColumn('ingestion_date', current_timestamp()) \
    .withColumn('data_source', lit(data_source)) \
    .withColumn('file_date', lit(file_date)) \
    .select('customer_id', 'event_start_time', 'event_type', 'rate_plan_id', 'billing_flag_1', 'billing_flag_2', 'duration', 'charge', 'ingestion_date', 'data_source', 'file_date')

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
telco_df.write \
    .mode ('overwrite') \
    .format ('delta') \
    .saveAsTable('default.silver_usage')

# COMMAND ----------

dbutils.notebook.exit('Success')
