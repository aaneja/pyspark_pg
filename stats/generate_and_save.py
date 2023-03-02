from datetime import datetime, date

import pyspark
from delta import *
from pyspark.sql import Row

# Create a SparkSessions with support for Delta tables
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/home/anant/Work/oss/aaneja/pyspark_pg/spark-warehouse") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

# Persist these to the warehouse
# PySpark will automatically instantiate a Apache Derby database (with HMS support)
# This can be examined with DBBeaver

# df.show()
# df.printSchema()

df.write.format('parquet').saveAsTable('tableParquet')
df.write.format('delta').saveAsTable('tableDelta')
spark.sql("SELECT count(*) from tableParquet").show()
spark.sql("SELECT count(*) from tableDelta").show()
