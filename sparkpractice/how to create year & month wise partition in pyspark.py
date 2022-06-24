# Databricks notebook source
df = spark.read.option("nullValue", "null").csv('/FileStore/tables/emp.csv', header=True, inferSchema=True)
df.show(10)

# COMMAND ----------

from pyspark.sql.functions import to_date
spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
df = df.withColumn('HIREDATE', to_date('HIREDATE', 'dd-MM-yy')).fillna({'HIREDATE':'9999-12-31'})
display(df)

# COMMAND ----------

from pyspark.sql.functions import date_format

df = df.withColumn('YEAR', date_format('HIREDATE', 'yyyy')).withColumn('MONTH', date_format('HIREDATE', 'MM')) 
display(df)                                                                       

# COMMAND ----------

df.write.format('delta').partitionBy('YEAR','MONTH').mode('overwrite').saveAsTable('emp_part')

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/emp_part/YEAR=1980/

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from emp_part where year = '1981'