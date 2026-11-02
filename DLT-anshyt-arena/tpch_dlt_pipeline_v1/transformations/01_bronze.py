import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.column import Column

# metadata add function
def add_metadata_column(df:DataFrame,source_name:str,source_path:Column =None) -> DataFrame:
    job_run_id = spark.conf.get("job_run_id", "Unknown")
    df = (
        df
        .withColumn("_ingestion_timestamp",current_timestamp())
        .withColumn("_source_path",source_path if source_path is not None else lit(source_name))
        .withColumn("_job_run_id",lit(job_run_id))
    )
    return df


# get and check up the configration 
CATALOG = spark.conf.get("catalog","dev_cat")
SCHEMA = spark.conf.get("schema","ops")
ENV = spark.conf.get("env","dev")

# source cat and schema
SOURCE_CATALOG = "samples"
SOURCE_SCHEMA  = "tpch"

# =====================================================
# BRONZE TABLE FACTORY
# 
# Creates a DLT Streaming Table for each source table with:
#   - Incremental streaming read (only new data processed per run)
#   - Change Data Feed (CDF) enabled → required for Silver's 
#     apply_changes() / SCD logic downstream
#   - Auto-optimize for small file compaction
#   - Standard audit columns
# =====================================================

def register_bronze_table(table_name:str):
    table_source_path = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}"

    # define table in decorator
    @dlt.table(
        name = f"{CATALOG}.bronze.{table_name}",
        comment = f"Raw data from {table_source_path}",
        table_properties = {
            # Enable CDF for change data feed
            "delta.enableChangeDataFeed" : "true",

            # auto optimize the table
            "delta.autoOptimize.optimizeWrite" : "true",
            "delta.autoOptimize.autoCompact" : "true"
        }
    )
    def _ingest():
        df = spark.readStream.table(table_source_path)
        return add_metadata_column(df,table_source_path)
    
    return _ingest

# All TPCH tables to ingest into bronze
TPCH_TABLES = [
    "customer",
    "orders",
    "lineitem",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region"
]

for _tbl in TPCH_TABLES:
    register_bronze_table(_tbl)


            

