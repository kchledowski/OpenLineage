# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if os.path.exists("/tmp/events.log"):
    os.remove("/tmp/events.log")

runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", None).replace(".", "_")

catalog = os.getenv("CATALOG", "openlineage_dev_catalog")
schema = os.getenv("SCHEMA", "ol_demo")
source_table = os.getenv("SOURCE_TABLE", "source_table")
derived_table = os.getenv("DERIVED_TABLE", "derived_table")

spark_builder = (
    SparkSession.builder.appName("ol-simple")
    .config(
        "spark.extraListeners",
        "io.openlineage.spark.agent.OpenLineageSparkListener,com.databricks.backend.daemon.driver.DBCEventLoggingListener",
    )
    .config("spark.openlineage.facets.debug.disabled", "false")
    .config("spark.openlineage.transport.type", "file")
    .config("spark.openlineage.transport.location", "/tmp/events.log")
)
spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("INFO")

spark.sql(f"USE CATALOG `{catalog}`")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

full_source = f"{catalog}.{schema}.{source_table}"
full_derived = f"{catalog}.{schema}.{derived_table}"

# Define explicit schema for consistency
demo_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
    ]
)

# Check if table exists and get row count
table_exists = spark.catalog.tableExists(f"{catalog}.{schema}.{source_table}")

if not table_exists:
    # Create table with demo data if it doesn't exist
    demo_df = spark.createDataFrame(
        [
            (1, "alpha", 10),
            (2, "beta", 20),
            (3, "gamma", 30),
            (4, "delta", 40),
        ],
        schema=demo_schema,
    )
    demo_df.write.format("delta").mode("overwrite").saveAsTable(full_source)
else:
    # Check if existing table is empty
    row_count = spark.table(full_source).count()
    if row_count == 0:
        # Table exists but is empty, append data
        demo_df = spark.createDataFrame(
            [
                (1, "alpha", 10),
                (2, "beta", 20),
                (3, "gamma", 30),
                (4, "delta", 40),
            ],
            schema=demo_schema,
        )
        demo_df.write.format("delta").mode("append").saveAsTable(full_source)
    else:
        print(f"Table {full_source} already exists with {row_count} rows")

# Read the source table
df = spark.table(full_source)

# Create a derived table (example: add computed column and aggregate)
derived_df = df.withColumn("value_x2", f.col("value") * f.lit(2))

# Write derived table (overwrite for idempotency)
derived_df.write.format("delta").mode("overwrite").saveAsTable(full_derived)

# Show a sample from derived table
spark.table(full_derived).show(10, truncate=False)

time.sleep(3)

event_file = "dbfs:/databricks/openlineage/events_{}.log".format(runtime_version)
dbutils.fs.rm(event_file, True)
dbutils.fs.cp("file:/tmp/events.log", event_file)
