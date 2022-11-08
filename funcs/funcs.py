# Databricks notebook source
# MAGIC %run "../constants/constants"

# COMMAND ----------

def unmount_adls(storage_acount_name, containers):
     '''
     Function takes Azure Blob Storage account name and list of containers that are needed.
     Containers are unmounted to match list.
     '''
    stripped_mounts = {i.split('/')[-1] for i in [c.mountPoint for c in dbutils.fs.mounts()]}
    mounts_to_remove = stripped_mounts - containers
    for container in mounts_to_remove:
        try:
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container}")
        except:
            pass

# COMMAND ----------

unmount_adls(storage_account_name, containers)

# COMMAND ----------

def mount_adls (storage_account, containers, secret):
      '''
     Function takes Azure Blob Storage account name and list of containers that are needed.
     Containers are mounted to match list.
     '''
    stripped_mounts = {i.split('/')[-1] for i in [c.mountPoint for c in dbutils.fs.mounts()]}
    new_mounts = containers.difference(stripped_mounts)
    for container in new_mounts:
        dbutils.fs.mount(
            source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
            mount_point = f"/mnt/{storage_account_name}/{container}",
            extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": secret})

# COMMAND ----------

mount_adls(storage_account_name, containers, secret)

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
     '''
     Function takes input dataframe, database and table names, folder path, merge column condition, 
     and partition parameters. Function returns new dataset if new data is added, appends if exists or updates 
     value if existed by merge condition.
     '''
    spark.conf.set('spark.databricks.optimizer.dynamicPruning', 'true')
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        deltaTable.alias('tgt').merge( \
            input_df.alias('src'),
            merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    else:
        input_df.write \
            .mode('overwrite') \
            .partitionBy(partition_column) \
            .format('delta') \
            .saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

def email_logs(message):
     '''
     Function takes message and sends it to recipients.
     '''
    import smtplib, ssl
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = sender  # Enter your address
    receiver_email = recipient  # Enter receiver address
    password = server_password
    message = message

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
