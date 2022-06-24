# Databricks notebook source
dbutils.fs.put('/FileStore/tables/skiplines.csv', """line1
line2
line3
line4
id name loc
1 abc Mumbai
2 pqr Pune
""",True)

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/skiplines.csv', header=True)
display(df)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/skiplines.csv')
rdd_final = rdd.zipWithIndex().filter(lambda x: x[1] > 3).map(lambda x: x[0].split(' '))
header = rdd_final.collect()[0]

# COMMAND ----------

rdd_final.filter(lambda x: x != header).toDF(header).show()

# COMMAND ----------

df = rdd_final.filter(lambda x: x != header).toDF(header)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Method2 : Using mapPartitionsWithIndex

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/skiplines.csv', 2).map(lambda x: x.split(','))
# rdd.mapPartitionsWithIndex(lambda idx, iter: iter if (idx == 0)).collect()
rdd.collect()

# COMMAND ----------

rdd3 = rdd.mapPartitionsWithIndex(lambda idx, iter: list(iter)[4:] if (idx == 0) else iter).map(lambda x: x[0].split(' '))
# rdd3.collect()
header = rdd3.collect()[0]
print(header)

# COMMAND ----------

df = rdd3.filter(lambda x: x!=header).toDF(header)
df.show()