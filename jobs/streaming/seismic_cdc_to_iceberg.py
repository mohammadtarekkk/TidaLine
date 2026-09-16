from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

# Debezium represents timestamps as microseconds from epoch (LongType) by default.
schema = StructType([
    StructField("unid", StringType(), True),
    StructField("source_id", StringType(), True),
    StructField("source_catalog", StringType(), True),
    StructField("lastupdate", LongType(), True),
    StructField("time", LongType(), True),
    StructField("flynn_region", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("evtype", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("mag", DoubleType(), True),
    StructField("magtype", StringType(), True),
    StructField("action", StringType(), True),
    StructField("received_at", LongType(), True),
    StructField("__deleted", StringType(), True) # Added by Debezium ExtractNewRecordState
])

spark = SparkSession.builder \
    .appName("Seismic CDC to Iceberg") \
    .getOrCreate()

# Ensure the Iceberg table exists
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.silver.earthquakes (
    unid STRING,
    source_id STRING,
    source_catalog STRING,
    lastupdate TIMESTAMP,
    time TIMESTAMP,
    flynn_region STRING,
    lat DOUBLE,
    lon DOUBLE,
    depth DOUBLE,
    evtype STRING,
    auth STRING,
    mag DOUBLE,
    magtype STRING,
    action STRING,
    received_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(time))
""")

# Read from Redpanda/Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "pg-server.public.earthquakes") \
    .option("startingOffsets", "earliest") \
    .load()

# The value is a JSON string. We parse it and apply some basic type casting for timestamps.
# Kafka messages include their own timestamp which we can use for ordering
parsed_df = df.selectExpr("CAST(value AS STRING)", "timestamp AS kafka_ts") \
    .filter(col("value").isNotNull()) \
    .select(from_json(col("value"), schema).alias("data"), col("kafka_ts")) \
    .select("data.*", "kafka_ts") \
    .filter(col("unid").isNotNull()) \
    .withColumn("lastupdate_ts", (col("lastupdate") / 1000000).cast("timestamp")) \
    .withColumn("time_ts", (col("time") / 1000000).cast("timestamp")) \
    .withColumn("received_at_ts", (col("received_at") / 1000000).cast("timestamp"))

def merge_batch(batch_df, batch_id):
    # Register the batch dataframe as a temporary view
    batch_df.createOrReplaceTempView("raw_batch")
    
    # Deduplicate in batch (keep latest event per unid based on Kafka timestamp)
    batch_df.sparkSession.sql("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY unid ORDER BY kafka_ts DESC) as rn
            FROM raw_batch
        ) WHERE rn = 1
    """).drop("rn", "kafka_ts").createOrReplaceTempView("batch_updates")
    
    # Use MERGE INTO to handle UPSERTs and DELETEs
    merge_query = """
    MERGE INTO iceberg.silver.earthquakes t
    USING batch_updates s
    ON t.unid = s.unid
    WHEN MATCHED AND s.__deleted = 'true' THEN DELETE
    WHEN MATCHED AND (s.__deleted IS NULL OR s.__deleted != 'true') THEN UPDATE SET
        t.source_id = s.source_id,
        t.source_catalog = s.source_catalog,
        t.lastupdate = s.lastupdate_ts,
        t.time = s.time_ts,
        t.flynn_region = s.flynn_region,
        t.lat = s.lat,
        t.lon = s.lon,
        t.depth = s.depth,
        t.evtype = s.evtype,
        t.auth = s.auth,
        t.mag = s.mag,
        t.magtype = s.magtype,
        t.action = s.action,
        t.received_at = s.received_at_ts
    WHEN NOT MATCHED AND (s.__deleted IS NULL OR s.__deleted != 'true') THEN INSERT (
        unid, source_id, source_catalog, lastupdate, time, flynn_region, lat, lon, depth, evtype, auth, mag, magtype, action, received_at
    ) VALUES (
        s.unid, s.source_id, s.source_catalog, s.lastupdate_ts, s.time_ts, s.flynn_region, s.lat, s.lon, s.depth, s.evtype, s.auth, s.mag, s.magtype, s.action, s.received_at_ts
    )
    """
    batch_df.sparkSession.sql(merge_query)

query = parsed_df.writeStream \
    .foreachBatch(merge_batch) \
    .option("checkpointLocation", "s3a://tidaline-lakehouse/checkpoints/earthquakes_cdc") \
    .start()

query.awaitTermination()
