# Databricks notebook source
dbutils.fs.put('/FileStore/tables/dynamic_columns.csv', """id, name, loc, email, phone
1, abc
2, pqr, Mumbai, pqr@gmail.com, 1234567890
3, xyz, Pune
""")

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/dynamic_columns.csv', header=True, inferSchema=True)
display(df)

# COMMAND ----------

dbutils.fs.put('/FileStore/tables/dynamic_columns_without_header.csv', """1, abc
2, pqr, Mumbai, pqr@gmail.com, 1234567890
3, xyz, Pune
""")

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/dynamic_columns_without_header.csv')
display(df)

# COMMAND ----------

df = spark.read.text('/FileStore/tables/dynamic_columns_without_header.csv')
display(df)

# COMMAND ----------

from pyspark.sql.functions import split
df = df.withColumn('splittable_col', split('value', ','))
display(df)

# COMMAND ----------

from pyspark.sql.functions import size, max
for i in range(df.select(max(size('splittable_col'))).collect()[0][0]):
    df = df.withColumn(f'col_{i}', df["splittable_col"][i])

display(df)
 

# COMMAND ----------

final_df = df.drop('value','splittable_col')
display(final_df)