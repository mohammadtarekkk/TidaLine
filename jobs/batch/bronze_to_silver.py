"""Bronze → Silver batch processing.

Reads raw CSVs from MinIO (Bronze layer), cleans column names,
casts date columns, and writes to Iceberg Silver tables.

Exit codes:
  0 — all tables processed successfully
  1 — one or more tables failed (logged with full traceback)
"""

import logging
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("bronze_to_silver")


def clean_column_names(df):
    """Normalize column names for Iceberg compatibility (lowercase, no spaces, no special chars)."""
    new_cols = []
    for c in df.columns:
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', c).strip('_').lower()
        clean_name = re.sub(r'_+', '_', clean_name)
        new_cols.append(col(c).alias(clean_name))
    return df.select(*new_cols)


def process_vessels(spark):
    """Read Bronze vessel CSVs, clean, and write to Silver Iceberg table."""
    logger.info("Processing Vessels...")
    vessels_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("s3a://tidaline-lakehouse/bronze/vessels/*.csv")

    row_count = vessels_df.count()
    if row_count == 0:
        raise ValueError("Vessels Bronze data is empty — aborting.")
    logger.info("Read %d rows from Bronze vessels.", row_count)

    vessels_df = clean_column_names(vessels_df)

    # Drop scraper metadata column that shouldn't leak into Silver
    if "detail_link" in vessels_df.columns:
        vessels_df = vessels_df.drop("detail_link")

    # Cast dates (Format from vessels.py: 'YYYY-MM-DD HH:MM')
    for date_col in ['departure_date', 'arrival_date', 'report_date']:
        if date_col in vessels_df.columns:
            vessels_df = vessels_df.withColumn(date_col, to_timestamp(col(date_col), "yyyy-MM-dd HH:mm"))

    # Cast numeric columns properly (replacing '-' with NULL)
    numeric_cols = ['gross_tonnage', 'deadweight', 'length_m', 'beam_m']
    for num_col in numeric_cols:
        if num_col in vessels_df.columns:
            vessels_df = vessels_df.withColumn(
                num_col, 
                when(col(num_col) == "-", None).otherwise(col(num_col)).cast("double")
            )
            
    if 'year_built' in vessels_df.columns:
        vessels_df = vessels_df.withColumn(
            'year_built', 
            when(col('year_built') == "-", None).otherwise(col('year_built')).cast("integer")
        )

    vessels_df = vessels_df.withColumn("processed_at", current_timestamp())

    # Deduplicate before writing, ignoring processed_at
    dedup_cols = [c for c in vessels_df.columns if c != "processed_at"]
    vessels_df = vessels_df.dropDuplicates(dedup_cols)

    if spark.catalog.tableExists("iceberg.silver.vessels"):
        vessels_df.writeTo("iceberg.silver.vessels").createOrReplace()
        logger.info("Replaced iceberg.silver.vessels with %d deduplicated rows", vessels_df.count())
    else:
        vessels_df.writeTo("iceberg.silver.vessels").tableProperty("format-version", "2").create()
        logger.info("Created iceberg.silver.vessels with %d deduplicated rows", vessels_df.count())


def process_ports(spark):
    """Read Bronze port CSVs, clean, and write to Silver Iceberg table."""
    logger.info("Processing Ports...")
    ports_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("s3a://tidaline-lakehouse/bronze/ports/*.csv")

    row_count = ports_df.count()
    if row_count == 0:
        raise ValueError("Ports Bronze data is empty — aborting.")
    logger.info("Read %d rows from Bronze ports.", row_count)

    ports_df = clean_column_names(ports_df)
    ports_df = ports_df.withColumn("processed_at", current_timestamp())

    if spark.catalog.tableExists("iceberg.silver.ports"):
        ports_df.writeTo("iceberg.silver.ports").createOrReplace()
        logger.info("Replaced iceberg.silver.ports with %d rows (dimension table)", row_count)
    else:
        ports_df.writeTo("iceberg.silver.ports").tableProperty("format-version", "2").create()
        logger.info("Created iceberg.silver.ports with %d rows", row_count)


def main():
    spark = SparkSession.builder \
        .appName("Bronze to Silver Batch") \
        .getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    failures = []

    for name, processor in [("vessels", process_vessels), ("ports", process_ports)]:
        try:
            processor(spark)
        except Exception:
            logger.exception("FAILED to process %s", name)
            failures.append(name)

    spark.stop()

    if failures:
        logger.error("Pipeline finished with failures: %s", ", ".join(failures))
        sys.exit(1)

    logger.info("Pipeline completed successfully — all tables processed.")


if __name__ == "__main__":
    main()
