# Databricks notebook source
dbutils.fs.put('/FileStore/tables/duplicates.csv', """id,name,location,update_date
1,Raj,Bangalore,2021-01-01
1,Raj,Mumbai,2022-01-01
1,Raj,Pune,2022-01-26
2,Ravi,Delhi,2022-01-01
2,Ravi,Hyderabad,2021-01-01
3,Kishan,Pune,2021-01-15
3,Kishan,Mumbai,2022-01-01
""", True)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/duplicates.csv', header=True)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date
df = df.withColumn('update_date', to_date('update_date', 'yyyy-MM-dd'))
df = df.orderBy(col('update_date').desc()).dropDuplicates(['id'])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Window function with row_number()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number
windowSpec = Window.partitionBy('id').orderBy(col('update_date').desc())

df2 = df.withColumn('rn', row_number().over(windowSpec)).where('rn==1').drop('rn')
display(df2)
