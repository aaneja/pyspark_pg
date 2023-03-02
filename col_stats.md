## Table parameters from the underlying Derby Metastore (HMS compat metastore)

```sql
SELECT * 
FROM APP.TABLE_PARAMS;
```

|TBL_ID|PARAM_KEY|PARAM_VALUE|
|------|---------|-----------|
|6|spark.sql.sources.provider|parquet|
|6|numFiles|4|
|6|totalSize|4919|
|6|spark.sql.create.version|3.3.2|
|6|spark.sql.sources.schema|{"type":"struct","fields":[{"name":"a","type":"long","nullable":true,"metadata":{}},{"name":"b","type":"double","nullable":true,"metadata":{}},{"name":"c","type":"string","nullable":true,"metadata":{}},{"name":"d","type":"date","nullable":true,"metadata":{}},{"name":"e","type":"timestamp","nullable":true,"metadata":{}}]}|
|6|transient_lastDdlTime|1677735681|
|6|spark.sql.statistics.numRows|3|
|6|spark.sql.statistics.totalSize|4919|
|6|spark.sql.statistics.colStats.a.version|2|
|6|spark.sql.statistics.colStats.d.min|2000-01-01|
|6|spark.sql.statistics.colStats.c.version|2|
|6|spark.sql.statistics.colStats.e.distinctCount|3|
|6|spark.sql.statistics.colStats.a.max|4|
|6|spark.sql.statistics.colStats.e.min|2000-01-01 06:30:00.000000|
|6|spark.sql.statistics.colStats.a.nullCount|0|
|6|spark.sql.statistics.colStats.b.version|2|
|6|spark.sql.statistics.colStats.c.distinctCount|3|
|6|spark.sql.statistics.colStats.b.max|5.0|
|6|spark.sql.statistics.colStats.b.distinctCount|3|
|6|spark.sql.statistics.colStats.e.nullCount|0|
|6|spark.sql.statistics.colStats.c.nullCount|0|
|6|spark.sql.statistics.colStats.b.min|2.0|
|6|spark.sql.statistics.colStats.d.distinctCount|3|
|6|spark.sql.statistics.colStats.e.maxLen|8|
|6|spark.sql.statistics.colStats.d.avgLen|4|
|6|spark.sql.statistics.colStats.e.avgLen|8|
|6|spark.sql.statistics.colStats.b.avgLen|8|
|6|spark.sql.statistics.colStats.c.avgLen|7|
|6|spark.sql.statistics.colStats.a.avgLen|8|
|6|spark.sql.statistics.colStats.d.nullCount|0|
|6|spark.sql.statistics.colStats.a.distinctCount|3|
|6|spark.sql.statistics.colStats.d.max|2000-03-01|
|6|spark.sql.statistics.colStats.a.min|1|
|6|spark.sql.statistics.colStats.a.maxLen|8|
|6|spark.sql.statistics.colStats.b.maxLen|8|
|6|spark.sql.statistics.colStats.b.nullCount|0|
|6|spark.sql.statistics.colStats.c.maxLen|7|
|6|spark.sql.statistics.colStats.d.version|2|
|6|spark.sql.statistics.colStats.e.max|2000-01-03 06:30:00.000000|
|6|spark.sql.statistics.colStats.d.maxLen|4|
|6|spark.sql.statistics.colStats.e.version|2|
|11|spark.sql.sources.schema|{"type":"struct","fields":[]}|
|11|numFiles|4|
|11|transient_lastDdlTime|1677735461|
|11|spark.sql.sources.provider|delta|
|11|totalSize|4919|
|11|spark.sql.create.version|3.3.2|
|11|spark.sql.partitionProvider|catalog|

## Points of note
- Stats are stored as table parameters with the prefix `spark.sql.statistics.colStats`
