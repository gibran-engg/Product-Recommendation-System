from pyspark.sql import SparkSession, functions as F

s = SparkSession.builder.master("local[1]").getOrCreate()
s.sparkContext.setLogLevel("ERROR")
df = s.read.parquet("hdfs://namenode:8020/data/raw/user_events")

n = df.count()
users = df.select("user_id").distinct().count()
items = df.select("item_id").distinct().count()
pairs = df.select("user_id", "item_id").distinct().count()

print("EVENTS", n)
print("USERS", users)
print("ITEMS", items)
print("UNIQUE user-item pairs", pairs)
print("DENSITY", pairs / (users * items))
df.groupBy("event_type").count().show()
print("NULLS PER COLUMN")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

im = s.read.parquet("hdfs://namenode:8020/data/processed/interaction_matrix")
print("INTERACTION MATRIX ROWS", im.count())
im.printSchema()
im.show(5)