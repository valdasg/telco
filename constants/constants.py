# Databricks notebook source
# Databricks notebook source
secret = dbutils.secrets.get(scope="myblob", key="accesskey")
sender = dbutils.secrets.get(scope="myblob", key="sender")
recipient = dbutils.secrets.get(scope="myblob", key="recipient")
server_password = dbutils.secrets.get(scope="myblob", key="server_password")
containers = {'raw', 'silver'}
storage_account_name = 'datavgsa'
