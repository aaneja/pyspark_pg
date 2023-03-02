import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/home/anant/Work/oss/aaneja/pyspark_pg/spark-warehouse") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())

table_name = "tablePartitionedParquet"

# ANALYZE on a PARTITION builds partition level size and row count
spark.sql("ANALYZE TABLE %s PARTITION (a = 3) COMPUTE STATISTICS" % table_name).show()
spark.sql("ANALYZE TABLE %s PARTITION (a = 2) COMPUTE STATISTICS" % table_name).show()
spark.sql("ANALYZE TABLE %s PARTITION (a = 1) COMPUTE STATISTICS" % table_name).show()
spark.sql("DESC EXTENDED %s PARTITION (a = 1)" % table_name).show(n=1000, truncate=False)
spark.sql("DESC EXTENDED %s PARTITION (a = 2)" % table_name).show(n=1000, truncate=False)
spark.sql("DESC EXTENDED %s PARTITION (a = 3)" % table_name).show(n=1000, truncate=False)

# This SQL executes but
# spark.sql("ANALYZE TABLE %s PARTITION (a = 2) COMPUTE STATISTICS FOR ALL COLUMNS" % table_name).show()
# https://spark.apache.org/docs/latest/sql-ref-syntax-aux-describe-table.html
# Below statement to get any column stats for a partition are not supported
# spark.sql("DESC TABLE %s PARTITION (a = 2) b" % table_name).show(n=1000, truncate=False)
