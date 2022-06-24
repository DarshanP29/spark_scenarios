# Databricks notebook source
# dbutils.fs.rm("/FileStore/tables/sample_excel.xls")
df = spark.read.format("com.crealytics.spark.excel").option("header","true").option("inferSchema","true").load('dbfs:/FileStore/tables/sample_excel.xlsx')
display(df)

# COMMAND ----------

df.write.format("com.crealytics.spark.excel").option("header","true").option("inferSchema","true").save("/FileStore/tables/sample_excel_write.xlsx",mode='overwrite')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/