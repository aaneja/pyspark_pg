import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport()

spark = builder.getOrCreate()
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())

df = spark.sql("select d_year from tpcds_sf1_parquet.date_dim")
execution = df._jdf.queryExecution()
# table_name = "tableParquet"
# spark.sql("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS" % table_name).show()
# spark.sql("DESC EXTENDED %s a" % table_name).show()
# spark.sql("DESC EXTENDED %s b" % table_name).show()
# spark.sql("DESC EXTENDED %s c" % table_name).show()
# spark.sql("DESC EXTENDED %s d" % table_name).show()
# spark.sql("DESC EXTENDED %s e" % table_name).show()

### Other commands
# df = spark.read.format("delta").load("spark-warehouse/tablea")
# spark.sql("SELECT count(*) from tableA").show()
# spark.sql("DROP TABLE tableA").show()

# spark.sql("DROP TABLE tableParquet").show()
# spark.sql("DROP TABLE tableDelta").show()