# Databricks notebook source
# Databricks notebook source
secret = dbutils.secrets.get(scope="myblob", key="accesskey")
containers = {'raw', 'silver'}
storage_account_name = 'datavgsa'
