import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from chispa.dataframe_comparer import assert_df_equality
import sys
import os

# Add the jobs directory to path so we can import the pipeline modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../jobs/batch')))
from bronze_to_silver import clean_column_names

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-spark") \
        .getOrCreate()

def test_clean_column_names(spark):
    # Given: a DataFrame with messy column names
    input_schema = StructType([
        StructField("Vessel Name", StringType(), True),
        StructField("IMO_Number", StringType(), True),
        StructField(" Gross Tonnage (GT)", IntegerType(), True),
        StructField("!@#_WEIRD___CoLuMn__", StringType(), True)
    ])
    input_data = [("Titanic", "1234567", 46000, "val")]
    input_df = spark.createDataFrame(input_data, schema=input_schema)

    # When: we apply the clean_column_names transformation
    result_df = clean_column_names(input_df)

    # Then: the columns should be properly formatted for Iceberg
    expected_schema = StructType([
        StructField("vessel_name", StringType(), True),
        StructField("imo_number", StringType(), True),
        StructField("gross_tonnage_gt", IntegerType(), True),
        StructField("weird_column", StringType(), True)
    ])
    expected_df = spark.createDataFrame(input_data, schema=expected_schema)

    assert_df_equality(result_df, expected_df)


