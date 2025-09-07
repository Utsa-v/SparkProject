from pyspark.sql import *
import pytest
from pyspark.sql.functions import col, expr

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
            .appName("BigDataTesting") \
            .master("local[*]") \
            .getOrCreate()
    return spark

@pytest.fixture(scope="module")
def sales_dataframes(spark):
    source_df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\source_sales.csv")
    target_df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\target_sales.csv")
    return source_df, target_df

@pytest.fixture(scope="module")
def large_sales_dataframes(spark):
    source_df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\source_sales.csv").repartition(8)
    target_df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Utsav Sharma\PycharmProjects\BigDataTesting\data\target_sales.csv").repartition(8)
    return source_df, target_df

def test_row_count_match(sales_dataframes):
    source_df, target_df = sales_dataframes
    assert source_df.count() == target_df.count(), "Row count mismatch"

def test_null_check(sales_dataframes):
    source_df, _ = sales_dataframes
    null_count = source_df.filter(col("order_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} NULL order_ids"

def test_transformation_logic(sales_dataframes):
    source_df, target_df = sales_dataframes

    # Add expected total column
    source_df = source_df.withColumn("expected_total", col("quantity") * col("price"))

    # Join and compare
    joined_df = source_df.join(target_df, on=["order_id", "product", "order_date"], how="inner")
    mismatches = joined_df.filter(col("expected_total") != col("total_amount"))

    count = mismatches.count()
    assert count == 0, f"Found {count} mismatches in transformation logic"

def test_duplicate_order_ids(sales_dataframes):
    source_df, _ = sales_dataframes
    from pyspark.sql.functions import count

    duplicates = source_df.groupBy("order_id").agg(count("*").alias("cnt")).filter("cnt > 1")
    count_dup = duplicates.count()
    assert count_dup == 0, f"Found {count_dup} duplicate order_ids"

def test_transformation_with_partitioning(large_sales_dataframes):
    source_df, target_df = large_sales_dataframes

    # Add expected total
    source_df = source_df.withColumn("expected_total", col("quantity") * col("price"))

    # Repartition both on join keys
    source_df = source_df.repartition("order_id")
    target_df = target_df.repartition("order_id")

    # Persist if reused multiple times
    source_df.persist()
    target_df.persist()

    joined_df = source_df.join(target_df, on=["order_id", "product", "order_date"], how="inner")

    mismatches = joined_df.filter(col("expected_total") != col("total_amount"))

    count = mismatches.count()
    assert count == 0, f"Found {count} mismatches with partitioned join"

    # Optional: Save mismatches
    if count > 0:
        mismatches.write.mode("overwrite").option("header", True).csv("logs/partitioned_mismatches")
