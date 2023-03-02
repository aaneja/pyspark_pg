## Spark per Partition stats

Spark stores partition stats as partition params. To generate these partition stats, run :

```
ANALYZE TABLE <tablename> PARTITION (<parition-col> = <partition-value>) COMPUTE STATISTICS
```

See [ANALYZE](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html) docs for more details

Then on querying the metastore (in our case, Derby) :

```sql
SELECT * 
FROM APP.PARTITION_PARAMS;
```

Results are:

| PART_ID | PARAM_KEY                      | PARAM_VALUE |
|---------|--------------------------------|-------------|
| 1       | numFiles                       | 2           |
| 1       | totalSize                      | 2422        |
| 1       | transient_lastDdlTime          | 1677741966  |
| 2       | totalSize                      | 2420        |
| 2       | numFiles                       | 2           |
| 2       | transient_lastDdlTime          | 1677741965  |
| 3       | numFiles                       | 2           |
| 3       | totalSize                      | 2422        |
| 3       | transient_lastDdlTime          | 1677741966  |
| 2       | spark.sql.statistics.numRows   | 2           |
| 2       | spark.sql.statistics.totalSize | 2420        |
| 3       | spark.sql.statistics.numRows   | 2           |
| 3       | spark.sql.statistics.totalSize | 2422        |
| 1       | spark.sql.statistics.numRows   | 2           |
| 1       | spark.sql.statistics.totalSize | 2422        |

Points of note

- Spark does track *overall* partition level stats. We see per partition entries for `spark.sql.statistics.numRows ` & `spark.sql.statistics.totalSize`. We get the same results
  on querying the data using `DESC EXTENDED <table-name> PARTITION (parition-col = partition-id)`
- Spark doesn't track partition level column stats ? I tried running `ANALYZE TABLE %s PARTITION (a = 1) COMPUTE STATISTICS FOR ALL COLUMNS` but this did not make any changed to
  the metastore