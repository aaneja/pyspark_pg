from datetime import datetime, date

import pyspark
from delta import *
from pyspark.sql import Row

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/home/anant/Work/oss/aaneja/pyspark_pg/spark-warehouse") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sql("DROP TABLE IF EXISTS tablePartitionedParquet")

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2001, 1, 1), e=datetime(2001, 1, 1, 12, 0)),
    Row(a=1, b=2.1, c='string1.1', d=date(2001, 1, 2), e=datetime(2001, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2002, 2, 1), e=datetime(2002, 1, 2, 12, 0)),
    Row(a=2, b=3.1, c='string2.1', d=date(2002, 2, 3), e=datetime(2002, 1, 2, 12, 0)),
    Row(a=3, b=5., c='string3', d=date(2003, 3, 1), e=datetime(2003, 1, 3, 12, 0)),
    Row(a=3, b=5.1, c='string3.1', d=date(2003, 3, 4), e=datetime(2003, 1, 3, 12, 0))
])

#Write as a partitioned table
df.write.format('parquet').partitionBy('a').saveAsTable('tablePartitionedParquet')
spark.sql("SELECT count(*) from tablePartitionedParquet").show()
