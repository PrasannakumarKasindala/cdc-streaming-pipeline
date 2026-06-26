"""The Spark job can't run without a cluster, but its correctness hinges on the
MERGE encoding the same LSN rule the merge core enforces. These checks pin that
down and ensure the module imports without pyspark installed."""

from cdcpipe import spark_job


def test_merge_sql_has_lsn_guard():
    sql = spark_job.MERGE_SQL
    # updates and deletes must be LSN-gated (cross-batch out-of-order safety)
    assert "s._lsn > t._lsn" in sql
    assert "WHEN MATCHED AND s.op = 'd' AND s._lsn > t._lsn THEN DELETE" in sql
    assert "WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT" in sql


def test_functions_exist():
    assert callable(spark_job.parse_debezium)
    assert callable(spark_job.dedup_to_latest)
    assert callable(spark_job.make_foreach_batch)


def test_debezium_schema_has_lsn_and_key():
    assert "lsn:bigint" in spark_job.DEBEZIUM_SCHEMA
    assert "order_id:bigint" in spark_job.DEBEZIUM_SCHEMA


def test_foreach_batch_is_closure():
    fn = spark_job.make_foreach_batch("lakehouse.orders")
    assert callable(fn)
