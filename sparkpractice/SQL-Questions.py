# Databricks notebook source
df = spark.read.option("nullValue", "null").csv('/FileStore/tables/emp.csv', header=True, inferSchema=True)
df.write.format('delta').mode('overwrite').saveAsTable('emp')
display(df.printSchema())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find min salary without using min function
# MAGIC select * from emp e where sal not in (
# MAGIC select e1.sal from emp e1, emp e2 where e1.sal > e2.sal)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find max salary without using max function
# MAGIC 
# MAGIC select * from emp where sal not in (
# MAGIC select e1.sal from emp e1, emp e2 where e1.sal < e2.sal)

# COMMAND ----------

# Find max salary without using max function

from pyspark.sql.functions import dense_rank, desc, col
from pyspark.sql.window import Window
df2 = df.withColumn('rk', dense_rank().over(Window.orderBy(col('SAL').desc()))).where('rk == 1')
display(df2)

# COMMAND ----------

# Find min salary without using min function
df3 = df.withColumn('rk', dense_rank().over(Window.orderBy(col('SAL')))).where('rk == 1')
display(df3)

# COMMAND ----------

# Find no. of rows in a dataframe without using count function

acc = spark.sparkContext.accumulator(0)
df.foreach(lambda x: acc.add(1))
print(acc.value)

# COMMAND ----------

# Find even or odd row from a table

df_even = df.withColumn('rn', row_number().over(Window.orderBy(col('SAL')))).where('rn % 2 == 0')
df_odd = df.withColumn('rn', row_number().over(Window.orderBy(col('SAL')))).where('rn % 2 != 0')
df_even.union(df_odd).show()