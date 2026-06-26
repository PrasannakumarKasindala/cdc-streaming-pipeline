"""Production Spark Structured Streaming job: Kafka (Debezium) -> Iceberg.

This is the deployment-time counterpart of the local pipeline. It is *not* run in
CI (no cluster in the sandbox); it is validated by a structure test that checks
the MERGE encodes the same rule the Python merge core enforces and is unit-tested:

    apply an event only when its source LSN is strictly greater than the LSN
    already recorded for that key.

Two things make the MERGE correct under at-least-once, out-of-order delivery:

1. **Per-batch dedup to the latest LSN per key.** A micro-batch can contain
   several events (and duplicates) for one key; collapse them to the max-LSN row
   first, so the MERGE sees one authoritative row per key.
2. **An LSN guard in the MERGE.** The target table carries an ``_lsn`` column;
   updates and deletes fire only when the incoming ``_lsn`` exceeds the stored
   one. That makes late (lower-LSN) events and re-deliveries no-ops -- the
   cross-batch, exactly-once behavior the row-number dedup alone can't give.

Run (Spark 3.5+, Iceberg + Kafka packages):

    spark-submit --packages \\
      org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\\
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
      spark_job.py --bootstrap kafka:9092 --topic dbserver.public.orders \\
      --table lakehouse.orders --checkpoint s3://.../ckpt
"""

from __future__ import annotations

# Target carries _lsn so ordering survives across micro-batches.
MERGE_SQL = """
MERGE INTO {table} t
USING batch s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd' AND s._lsn > t._lsn THEN DELETE
WHEN MATCHED AND s.op <> 'd' AND s._lsn > t._lsn THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT *
"""

DEBEZIUM_SCHEMA = (
    "struct<"
    "op:string,"
    "ts_ms:bigint,"
    "before:struct<order_id:bigint>,"
    "after:struct<order_id:bigint,customer_id:bigint,status:string,"
    "amount:decimal(18,2),updated_ms:bigint>,"
    "source:struct<lsn:bigint>"
    ">"
)


def parse_debezium(kafka_df):
    """Kafka value bytes -> flat rows with op, _lsn, and the after-image columns."""
    from pyspark.sql import functions as F

    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), DEBEZIUM_SCHEMA).alias("e")
    )
    return parsed.select(
        F.col("e.op").alias("op"),
        F.col("e.source.lsn").alias("_lsn"),
        F.coalesce(F.col("e.after.order_id"), F.col("e.before.order_id")).alias("order_id"),
        F.col("e.after.customer_id").alias("customer_id"),
        F.col("e.after.status").alias("status"),
        F.col("e.after.amount").alias("amount"),
        F.col("e.after.updated_ms").alias("updated_ms"),
    )


def dedup_to_latest(df):
    """Collapse a micro-batch to one row per key: the highest-LSN event."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("order_id").orderBy(F.col("_lsn").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1).drop("_rn"))


def make_foreach_batch(table: str):
    def upsert(batch_df, _epoch_id):
        latest = dedup_to_latest(batch_df)
        latest.sparkSession.catalog.dropTempView("batch")
        latest.createOrReplaceTempView("batch")
        latest.sparkSession.sql(MERGE_SQL.format(table=table))
    return upsert


def main(argv=None):  # pragma: no cover - requires a Spark cluster
    import argparse

    from pyspark.sql import SparkSession

    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--checkpoint", required=True)
    ap.add_argument("--starting-offsets", default="earliest")
    args = ap.parse_args(argv)

    spark = (SparkSession.builder.appName("cdc-streaming-pipeline")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions."
                     "IcebergSparkSessionExtensions")
             .getOrCreate())

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("subscribe", args.topic)
                .option("startingOffsets", args.starting_offsets)
                .load())

    query = (parse_debezium(kafka_df).writeStream
             .foreachBatch(make_foreach_batch(args.table))
             .option("checkpointLocation", args.checkpoint)  # exactly-once source offsets
             .outputMode("update")
             .start())
    query.awaitTermination()


if __name__ == "__main__":  # pragma: no cover
    main()
