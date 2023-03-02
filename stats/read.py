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

table_name = "tableParquet"
spark.sql("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS" % table_name).show()
spark.sql("DESC EXTENDED %s a" % table_name).show()
spark.sql("DESC EXTENDED %s b" % table_name).show()
spark.sql("DESC EXTENDED %s c" % table_name).show()
spark.sql("DESC EXTENDED %s d" % table_name).show()
spark.sql("DESC EXTENDED %s e" % table_name).show()

## tableDelta : Delta is a 'v2' table and ANALYZE on it is only supported with the Databricks Runtime 8.3 and above
# pyspark.sql.utils.AnalysisException: ANALYZE TABLE is not supported for v2 tables.
# Underlying JVM exception is
# An error occurred while calling o37.sql.
# : org.apache.spark.sql.AnalysisException: ANALYZE TABLE is not supported for v2 tables.
#     at org.apache.spark.sql.errors.QueryCompilationErrors$.notSupportedForV2TablesError(QueryCompilationErrors.scala:957)
#     at org.apache.spark.sql.errors.QueryCompilationErrors$.analyzeTableNotSupportedForV2TablesError(QueryCompilationErrors.scala:961)
#     at org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy.apply(DataSourceV2Strategy.scala:372)
#     at org.apache.spark.sql.catalyst.planning.QueryPlanner.$anonfun$plan$1(QueryPlanner.scala:63)
#     at scala.collection.Iterator$$anon$11.nextCur(Iterator.scala:486)
#     at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:492)
#     at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:491)
#     at org.apache.spark.sql.catalyst.planning.QueryPlanner.plan(QueryPlanner.scala:93)
#     at org.apache.spark.sql.execution.SparkStrategies.plan(SparkStrategies.scala:69)
#     at org.apache.spark.sql.catalyst.planning.QueryPlanner.$anonfun$plan$3(QueryPlanner.scala:78)
#     at scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
#     at scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:194)
#     at scala.collection.Iterator.foreach(Iterator.scala:943)
#     at scala.collection.Iterator.foreach$(Iterator.scala:943)
#     at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
#     at scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:199)
#     at scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:192)
#     at scala.collection.AbstractIterator.foldLeft(Iterator.scala:1431)
#     at org.apache.spark.sql.catalyst.planning.QueryPlanner.$anonfun$plan$2(QueryPlanner.scala:75)
#     at scala.collection.Iterator$$anon$11.nextCur(Iterator.scala:486)
#     at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:492)
#     at org.apache.spark.sql.catalyst.planning.QueryPlanner.plan(QueryPlanner.scala:93)
#     at org.apache.spark.sql.execution.SparkStrategies.plan(SparkStrategies.scala:69)
#     at org.apache.spark.sql.execution.QueryExecution$.createSparkPlan(QueryExecution.scala:459)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$sparkPlan$1(QueryExecution.scala:145)
#     at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:185)
#     at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:510)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:185)
#     at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
#     at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:184)
#     at org.apache.spark.sql.execution.QueryExecution.sparkPlan$lzycompute(QueryExecution.scala:145)
#     at org.apache.spark.sql.execution.QueryExecution.sparkPlan(QueryExecution.scala:138)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$executedPlan$1(QueryExecution.scala:158)
#     at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:185)
#     at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:510)
#     at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:185)
#     at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
#     at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:184)
#     at org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:158)
#     at org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:151)
#     at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:106)
#     at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:169)
#     at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:95)
#     at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
#     at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
#     at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
#     at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:94)
#     at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:584)
#     at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:176)
#     at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:584)
#     at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:30)
#     at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
#     at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
#     at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
#     at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
#     at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:560)
#     at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:94)
#     at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:81)
#     at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:79)
#     at org.apache.spark.sql.Dataset.<init>(Dataset.scala:220)
#     at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:100)
#     at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
#     at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:97)
#     at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:622)
#     at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
#     at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:617)
#     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
#     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
#     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
#     at java.lang.reflect.Method.invoke(Method.java:498)
#     at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
#     at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
#     at py4j.Gateway.invoke(Gateway.java:282)
#     at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
#     at py4j.commands.CallCommand.execute(CallCommand.java:79)
#     at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
#     at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
#     at java.lang.Thread.run(Thread.java:750)


### tableParquet is a 'vanilla' parquet table. Stats are supported

# +--------------+----------+
# |     info_name|info_value|
# +--------------+----------+
# |      col_name|         a|
# |     data_type|    bigint|
# |       comment|      NULL|
# |           min|         1|
# |           max|         4|
# |     num_nulls|         0|
# |distinct_count|         3|
# |   avg_col_len|         8|
# |   max_col_len|         8|
# |     histogram|      NULL|
# +--------------+----------+
#
# +--------------+----------+
# |     info_name|info_value|
# +--------------+----------+
# |      col_name|         b|
# |     data_type|    double|
# |       comment|      NULL|
# |           min|       2.0|
# |           max|       5.0|
# |     num_nulls|         0|
# |distinct_count|         3|
# |   avg_col_len|         8|
# |   max_col_len|         8|
# |     histogram|      NULL|
# +--------------+----------+
#
# +--------------+----------+
# |     info_name|info_value|
# +--------------+----------+
# |      col_name|         c|
# |     data_type|    string|
# |       comment|      NULL|
# |           min|      NULL|
# |           max|      NULL|
# |     num_nulls|         0|
# |distinct_count|         3|
# |   avg_col_len|         7|
# |   max_col_len|         7|
# |     histogram|      NULL|
# +--------------+----------+
#
# +--------------+----------+
# |     info_name|info_value|
# +--------------+----------+
# |      col_name|         d|
# |     data_type|      date|
# |       comment|      NULL|
# |           min|2000-01-01|
# |           max|2000-03-01|
# |     num_nulls|         0|
# |distinct_count|         3|
# |   avg_col_len|         4|
# |   max_col_len|         4|
# |     histogram|      NULL|
# +--------------+----------+
#
# +--------------+--------------------+
# |     info_name|          info_value|
# +--------------+--------------------+
# |      col_name|                   e|
# |     data_type|           timestamp|
# |       comment|                NULL|
# |           min|2000-01-01 06:30:...|
# |           max|2000-01-03 06:30:...|
# |     num_nulls|                   0|
# |distinct_count|                   3|
# |   avg_col_len|                   8|
# |   max_col_len|                   8|
# |     histogram|                NULL|
# +--------------+--------------------+


### Other commands
# df = spark.read.format("delta").load("spark-warehouse/tablea")
# spark.sql("SELECT count(*) from tableA").show()
# spark.sql("DROP TABLE tableA").show()

# spark.sql("DROP TABLE tableParquet").show()
# spark.sql("DROP TABLE tableDelta").show()