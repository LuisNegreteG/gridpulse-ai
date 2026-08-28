# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "09ba38d0-4e10-422e-a1ab-e5d75b1a0160",
# META       "default_lakehouse_name": "lh_gridpulse",
# META       "default_lakehouse_workspace_id": "76887489-1772-4da4-9b27-184dff4f24b9",
# META       "known_lakehouses": [
# META         {
# META           "id": "09ba38d0-4e10-422e-a1ab-e5d75b1a0160"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

print("Silver notebook initialized.")
print("Spark session timezone: UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.silver as silver

silver = importlib.reload(silver)

current_demand_payloads = silver.get_current_successful_payloads(
    spark=spark,
    source_name="ieso_hourly_demand",
)

rows = current_demand_payloads.collect()

assert len(rows) == 1
assert rows[0]["processing_status"] == "SUCCESS"
assert rows[0]["bronze_path"] is not None

print("Current Bronze payload selection passed.")
print(f"Logical source key: {rows[0]['logical_source_key']}")
print(f"Source hash: {rows[0]['source_hash']}")
print(f"Bronze path: {rows[0]['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.silver as silver
import builtin.gridpulse.parsers.demand as demand_parser

silver = importlib.reload(silver)
demand_parser = importlib.reload(demand_parser)


current_payload = (
    silver.get_current_successful_payloads(
        spark=spark,
        source_name="ieso_hourly_demand",
    )
    .collect()
)

assert len(current_payload) == 1

demand_registry_row = current_payload[0]

demand_raw_df = demand_parser.parse_hourly_demand(
    spark=spark,
    bronze_path=demand_registry_row["bronze_path"],
)

print(f"Parsed rows: {demand_raw_df.count():,}")
demand_raw_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_columns = [
    "date_raw",
    "hour_raw",
    "market_demand_raw",
    "ontario_demand_raw",
    "_source_row_number",
]

assert demand_raw_df.columns == expected_columns
assert demand_raw_df.count() > 0

print("SRC-001 raw parser validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.parsers.demand as demand_parser
import builtin.gridpulse.dq as dq

demand_parser = importlib.reload(demand_parser)
dq = importlib.reload(dq)


demand_typed_df = demand_parser.type_hourly_demand(
    demand_raw_df
)

dq_results = dq.validate_hourly_demand(
    demand_typed_df
)

print("SRC-001 DQ validation passed.")

for rule, failed_count in dq_results.items():
    print(f"{rule}: {failed_count} failures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_silver_df = (
    demand_typed_df
    .select(
        "market_date",
        "hour_ending",
        "market_demand_mw",
        "ontario_demand_mw",
    )
)

demand_silver_df = silver.add_lineage_columns(
    demand_silver_df,
    demand_registry_row,
)

demand_silver_df.printSchema()
demand_silver_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create silver.demand_hourly + Delta MERGE

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.demand_hourly (
# MAGIC     market_date DATE NOT NULL,
# MAGIC     hour_ending INT NOT NULL,
# MAGIC     market_demand_mw DECIMAL(18,3) NOT NULL,
# MAGIC     ontario_demand_mw DECIMAL(18,3) NOT NULL,
# MAGIC     _source_name STRING NOT NULL,
# MAGIC     _source_file STRING NOT NULL,
# MAGIC     _source_url STRING NOT NULL,
# MAGIC     _source_hash STRING NOT NULL,
# MAGIC     _source_version STRING,
# MAGIC     _source_created_at STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP NOT NULL,
# MAGIC     _run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS silver_rows
# MAGIC FROM silver.demand_hourly;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.silver as silver

silver = importlib.reload(silver)

silver.merge_into_silver(
    spark=spark,
    source_df=demand_silver_df,
    target_table="silver.demand_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
    ],
)

print("SRC-001 Silver MERGE completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

target_df = spark.table(
    "silver.demand_hourly"
)

duplicate_keys = (
    target_df
    .groupBy(
        "market_date",
        "hour_ending",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_source_keys = (
    demand_silver_df
    .select(
        "market_date",
        "hour_ending",
    )
    .join(
        target_df.select(
            "market_date",
            "hour_ending",
        ),
        on=[
            "market_date",
            "hour_ending",
        ],
        how="left_anti",
    )
    .count()
)

assert duplicate_keys == 0
assert missing_source_keys == 0

print(f"Silver rows: {target_df.count():,}")
print(f"Duplicate business keys: {duplicate_keys}")
print(f"Missing incoming keys: {missing_source_keys}")
print("SRC-001 Silver validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

target_before = spark.table("silver.demand_hourly")
rows_before = target_before.count()

silver.merge_into_silver(
    spark=spark,
    source_df=demand_silver_df,
    target_table="silver.demand_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
    ],
)

target_after = spark.table("silver.demand_hourly")
rows_after = target_after.count()

duplicate_keys = (
    target_after
    .groupBy("market_date", "hour_ending")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert rows_after == rows_before
assert duplicate_keys == 0

print(f"Rows before rerun: {rows_before:,}")
print(f"Rows after rerun:  {rows_after:,}")
print(f"Duplicate keys:    {duplicate_keys}")
print("SRC-001 Silver idempotency validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

comparison = (
    demand_silver_df.alias("source")
    .join(
        target_after.alias("target"),
        on=["market_date", "hour_ending"],
        how="inner",
    )
    .filter(
        (F.col("source.market_demand_mw") != F.col("target.market_demand_mw"))
        | (F.col("source.ontario_demand_mw") != F.col("target.ontario_demand_mw"))
        | (F.col("source._source_hash") != F.col("target._source_hash"))
        | (F.col("source._run_id") != F.col("target._run_id"))
    )
    .count()
)

assert comparison == 0

print(f"Mismatched current rows: {comparison}")
print("SRC-001 value and lineage validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.silver as silver
import builtin.gridpulse.parsers.demand_zonal as zonal_parser

silver = importlib.reload(silver)
zonal_parser = importlib.reload(zonal_parser)


current_zonal_payload = (
    silver.get_current_successful_payloads(
        spark=spark,
        source_name="ieso_hourly_zonal_demand",
    )
    .collect()
)

assert len(current_zonal_payload) == 1

zonal_registry_row = current_zonal_payload[0]

zonal_raw_df = zonal_parser.parse_hourly_zonal_demand(
    spark=spark,
    bronze_path=zonal_registry_row["bronze_path"],
)

print(f"Physical source rows: {zonal_raw_df.count():,}")
zonal_raw_df.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_typed_df = zonal_parser.type_hourly_zonal_demand(
    zonal_raw_df
)

zonal_unpivoted_df = zonal_parser.unpivot_zonal_demand(
    zonal_typed_df
)

physical_rows = zonal_typed_df.count()
silver_candidate_rows = zonal_unpivoted_df.count()

assert silver_candidate_rows == physical_rows * 10

print(f"Physical rows: {physical_rows:,}")
print(f"Unpivoted rows: {silver_candidate_rows:,}")
print("SRC-002 unpivot validation passed.")

zonal_unpivoted_df.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.dq as dq

dq = importlib.reload(dq)

zonal_dq_results = dq.validate_hourly_zonal_demand(
    typed_df=zonal_typed_df,
    unpivoted_df=zonal_unpivoted_df,
)

print("SRC-002 FAIL-level DQ validation passed.")

print("\nFAIL rules:")
for rule, count in zonal_dq_results["fail"].items():
    print(f"{rule}: {count} failures")

print("\nWARN observations:")
for rule, count in zonal_dq_results["warn"].items():
    print(f"{rule}: {count} observations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_silver_df = silver.add_lineage_columns(
    zonal_unpivoted_df,
    zonal_registry_row,
)

zonal_silver_df.printSchema()
zonal_silver_df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Table Silver Demand Zonal HOurly

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.demand_zonal_hourly (
# MAGIC     market_date DATE NOT NULL,
# MAGIC     hour_ending INT NOT NULL,
# MAGIC     zone STRING NOT NULL,
# MAGIC     zonal_demand_mw DECIMAL(18,3) NOT NULL,
# MAGIC     _source_name STRING NOT NULL,
# MAGIC     _source_file STRING NOT NULL,
# MAGIC     _source_url STRING NOT NULL,
# MAGIC     _source_hash STRING NOT NULL,
# MAGIC     _source_version STRING,
# MAGIC     _source_created_at STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP NOT NULL,
# MAGIC     _run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS silver_rows
# MAGIC FROM silver.demand_zonal_hourly;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.merge_into_silver(
    spark=spark,
    source_df=zonal_silver_df,
    target_table="silver.demand_zonal_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
        "zone",
    ],
)

print("SRC-002 Silver MERGE completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

zonal_target_df = spark.table(
    "silver.demand_zonal_hourly"
)

duplicate_keys = (
    zonal_target_df
    .groupBy(
        "market_date",
        "hour_ending",
        "zone",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_keys = (
    zonal_silver_df
    .select(
        "market_date",
        "hour_ending",
        "zone",
    )
    .join(
        zonal_target_df.select(
            "market_date",
            "hour_ending",
            "zone",
        ),
        on=[
            "market_date",
            "hour_ending",
            "zone",
        ],
        how="left_anti",
    )
    .count()
)

assert duplicate_keys == 0
assert missing_keys == 0

print(f"Silver rows: {zonal_target_df.count():,}")
print(f"Duplicate business keys: {duplicate_keys}")
print(f"Missing incoming keys: {missing_keys}")
print("SRC-002 Silver validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_current_df = (
    spark.table("silver.demand_hourly")
    .select(
        "market_date",
        "hour_ending",
        F.col("ontario_demand_mw").alias(
            "src001_ontario_demand_mw"
        ),
    )
)

zonal_aggregate_df = (
    zonal_typed_df
    .select(
        "market_date",
        "hour_ending",
        F.col("ontario_demand_mw").alias(
            "src002_ontario_demand_mw"
        ),
    )
)

reconciliation_df = (
    demand_current_df.alias("demand")
    .join(
        zonal_aggregate_df.alias("zonal"),
        on=["market_date", "hour_ending"],
        how="inner",
    )
    .withColumn(
        "difference_mw",
        F.col("src001_ontario_demand_mw")
        - F.col("src002_ontario_demand_mw"),
    )
)

overlap_count = reconciliation_df.count()

mismatch_df = (
    reconciliation_df
    .filter(F.col("difference_mw") != 0)
)

mismatch_count = mismatch_df.count()

assert overlap_count > 0

print(f"Overlapping records: {overlap_count:,}")
print(f"Cross-source mismatches (WARN): {mismatch_count:,}")

mismatch_df.orderBy(
    "market_date",
    "hour_ending",
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.silver as silver
import builtin.gridpulse.parsers.generation as generation_parser

silver = importlib.reload(silver)
generation_parser = importlib.reload(generation_parser)


current_generation_payload = (
    silver.get_current_successful_payloads(
        spark=spark,
        source_name="ieso_generation_by_fuel_hourly",
    )
    .collect()
)

assert len(current_generation_payload) == 1

generation_registry_row = current_generation_payload[0]

generation_raw_df = (
    generation_parser.parse_generation_hourly(
        spark=spark,
        bronze_path=generation_registry_row["bronze_path"],
    )
)

print(f"Flattened generation rows: {generation_raw_df.count():,}")

generation_raw_df.show(
    20,
    truncate=False,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_columns = [
    "date_raw",
    "hour_raw",
    "fuel_type_raw",
    "output_raw",
    "output_quality_raw",
    "_source_record_number",
]

assert generation_raw_df.columns == expected_columns
assert generation_raw_df.count() > 0

print("SRC-003 XML parser validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_raw_df.select(
    "fuel_type_raw"
).distinct().orderBy(
    "fuel_type_raw"
).show(
    truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.parsers.generation as generation_parser
import builtin.gridpulse.dq as dq

generation_parser = importlib.reload(generation_parser)
dq = importlib.reload(dq)


generation_typed_df = generation_parser.type_generation_hourly(
    generation_raw_df
)

generation_dq_results = dq.validate_generation_hourly(
    generation_typed_df
)

print("SRC-003 FAIL-level DQ validation passed.")

print("\nFAIL rules:")
for rule, count in generation_dq_results["fail"].items():
    print(f"{rule}: {count} failures")

print("\nObservations:")
for rule, count in generation_dq_results["observations"].items():
    print(f"{rule}: {count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_typed_df.select(
    "output_quality_code"
).distinct().orderBy(
    "output_quality_code"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_typed_df.filter(
    F.col("output_mwh").isNull()
).select(
    "market_date",
    "hour_ending",
    "fuel_type",
    "output_mwh",
    "output_quality_code",
).show(
    30,
    truncate=False,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_silver_df = (
    generation_typed_df
    .select(
        "market_date",
        "hour_ending",
        "fuel_type",
        "output_mwh",
        "output_quality_code",
    )
)

generation_silver_df = silver.add_lineage_columns(
    generation_silver_df,
    generation_registry_row,
)

generation_silver_df.printSchema()
generation_silver_df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Table Generation Hourly

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.generation_hourly (
# MAGIC     market_date DATE NOT NULL,
# MAGIC     hour_ending INT NOT NULL,
# MAGIC     fuel_type STRING NOT NULL,
# MAGIC     output_mwh DECIMAL(18,3),
# MAGIC     output_quality_code INT NOT NULL,
# MAGIC     _source_name STRING NOT NULL,
# MAGIC     _source_file STRING NOT NULL,
# MAGIC     _source_url STRING NOT NULL,
# MAGIC     _source_hash STRING NOT NULL,
# MAGIC     _source_version STRING,
# MAGIC     _source_created_at STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP NOT NULL,
# MAGIC     _run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.merge_into_silver(
    spark=spark,
    source_df=generation_silver_df,
    target_table="silver.generation_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
        "fuel_type",
    ],
)

print("SRC-003 Silver MERGE completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

generation_target_df = spark.table(
    "silver.generation_hourly"
)

duplicate_keys = (
    generation_target_df
    .groupBy(
        "market_date",
        "hour_ending",
        "fuel_type",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_keys = (
    generation_silver_df
    .select(
        "market_date",
        "hour_ending",
        "fuel_type",
    )
    .join(
        generation_target_df.select(
            "market_date",
            "hour_ending",
            "fuel_type",
        ),
        on=[
            "market_date",
            "hour_ending",
            "fuel_type",
        ],
        how="left_anti",
    )
    .count()
)

assert duplicate_keys == 0
assert missing_keys == 0

print(f"Silver rows: {generation_target_df.count():,}")
print(f"Duplicate business keys: {duplicate_keys}")
print(f"Missing incoming keys: {missing_keys}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_nulls = (
    generation_silver_df
    .filter(F.col("output_mwh").isNull())
    .count()
)

target_nulls_for_current_payload = (
    generation_target_df
    .filter(
        (F.col("_source_hash") == generation_registry_row["source_hash"])
        & F.col("output_mwh").isNull()
    )
    .count()
)

assert source_nulls == target_nulls_for_current_payload

print(f"Source null outputs: {source_nulls}")
print(f"Silver null outputs: {target_nulls_for_current_payload}")
print("SRC-003 null preservation validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.silver as silver
import builtin.gridpulse.parsers.price_day_ahead as da_parser

silver = importlib.reload(silver)
da_parser = importlib.reload(da_parser)


current_da_payload = (
    silver.get_current_successful_payloads(
        spark=spark,
        source_name="ieso_day_ahead_ontario_zonal_price",
    )
    .collect()
)

assert len(current_da_payload) == 1

da_registry_row = current_da_payload[0]

da_raw_df = da_parser.parse_day_ahead_price(
    spark=spark,
    bronze_path=da_registry_row["bronze_path"],
)

print(f"Parsed Day-Ahead rows: {da_raw_df.count():,}")
da_raw_df.show(24, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_typed_df = da_parser.type_day_ahead_price(
    da_raw_df
)

da_typed_df.select(
    "market_date",
    "hour_ending",
    "zonal_price_cad_per_mwh",
    "loss_price_capped_cad_per_mwh",
    "congestion_price_capped_cad_per_mwh",
    "source_flag",
).show(
    24,
    truncate=False,
)

print("SRC-004 parser and typing validation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.dq as dq

dq = importlib.reload(dq)

da_dq_results = dq.validate_day_ahead_price(
    da_typed_df
)

print("SRC-004 FAIL-level DQ validation passed.")

print("\nFAIL rules:")
for rule, count in da_dq_results["fail"].items():
    print(f"{rule}: {count} failures")

print("\nObservations:")
for rule, count in da_dq_results["observations"].items():
    print(f"{rule}: {count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_silver_df = (
    da_typed_df
    .select(
        "market_date",
        "hour_ending",
        "zonal_price_cad_per_mwh",
        "loss_price_capped_cad_per_mwh",
        "congestion_price_capped_cad_per_mwh",
        "source_flag",
    )
)

da_silver_df = silver.add_lineage_columns(
    da_silver_df,
    da_registry_row,
)

da_silver_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create table Price Ahead Hourly

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.price_day_ahead_hourly (
# MAGIC     market_date DATE NOT NULL,
# MAGIC     hour_ending INT NOT NULL,
# MAGIC     zonal_price_cad_per_mwh DECIMAL(18,2),
# MAGIC     loss_price_capped_cad_per_mwh DECIMAL(18,2),
# MAGIC     congestion_price_capped_cad_per_mwh DECIMAL(18,2),
# MAGIC     source_flag STRING,
# MAGIC     _source_name STRING NOT NULL,
# MAGIC     _source_file STRING NOT NULL,
# MAGIC     _source_url STRING NOT NULL,
# MAGIC     _source_hash STRING NOT NULL,
# MAGIC     _source_version STRING,
# MAGIC     _source_created_at STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP NOT NULL,
# MAGIC     _run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.merge_into_silver(
    spark=spark,
    source_df=da_silver_df,
    target_table="silver.price_day_ahead_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
    ],
)

print("SRC-004 Silver MERGE completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_target_df = spark.table(
    "silver.price_day_ahead_hourly"
)

duplicate_keys = (
    da_target_df
    .groupBy(
        "market_date",
        "hour_ending",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_keys = (
    da_silver_df
    .select(
        "market_date",
        "hour_ending",
    )
    .join(
        da_target_df.select(
            "market_date",
            "hour_ending",
        ),
        on=[
            "market_date",
            "hour_ending",
        ],
        how="left_anti",
    )
    .count()
)

assert duplicate_keys == 0
assert missing_keys == 0

print(f"Silver rows: {da_target_df.count():,}")
print(f"Duplicate business keys: {duplicate_keys}")
print(f"Missing incoming keys: {missing_keys}")
print("SRC-004 Silver validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.silver as silver
import builtin.gridpulse.parsers.price_realtime as rt_parser

silver = importlib.reload(silver)
rt_parser = importlib.reload(rt_parser)


current_rt_payload = (
    silver.get_current_successful_payloads(
        spark=spark,
        source_name="ieso_realtime_ontario_zonal_price",
    )
    .collect()
)

assert len(current_rt_payload) == 1

rt_registry_row = current_rt_payload[0]

rt_raw_df = rt_parser.parse_realtime_price(
    spark=spark,
    bronze_path=rt_registry_row["bronze_path"],
)

print(f"Parsed RT interval slots: {rt_raw_df.count()}")

rt_raw_df.show(
    20,
    truncate=False,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_typed_df = rt_parser.type_realtime_price(
    rt_raw_df
)

rt_typed_df.select(
    "delivery_date",
    "delivery_hour",
    "interval",
    "zonal_price_capped_cad_per_mwh",
    "loss_price_capped_cad_per_mwh",
    "congestion_price_capped_cad_per_mwh",
    "source_flag",
    "created_at_raw",
).orderBy(
    "interval"
).show(
    20,
    truncate=False,
)

print("SRC-005 parser and typing validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.dq as dq

dq = importlib.reload(dq)

rt_dq_results = dq.validate_realtime_price(
    rt_typed_df
)

print("SRC-005 FAIL-level DQ validation passed.")

print("\nFAIL rules:")
for rule, count in rt_dq_results["fail"].items():
    print(f"{rule}: {count} failures")

print("\nWARN:")
for rule, count in rt_dq_results["warn"].items():
    print(f"{rule}: {count}")

print("\nObservations:")
for rule, count in rt_dq_results["observations"].items():
    print(f"{rule}: {count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_silver_df = (
    rt_typed_df
    .select(
        "delivery_date",
        "delivery_hour",
        "interval",
        "zonal_price_capped_cad_per_mwh",
        "loss_price_capped_cad_per_mwh",
        "congestion_price_capped_cad_per_mwh",
        "source_flag",
        "created_at_raw",
    )
)

rt_silver_df = silver.add_lineage_columns(
    rt_silver_df,
    rt_registry_row,
)

rt_silver_df = (
    rt_silver_df
    .withColumn(
        "_source_created_at",
        F.col("created_at_raw"),
    )
    .drop("created_at_raw")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create table for RT 5m

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.price_realtime_5min (
# MAGIC     delivery_date DATE NOT NULL,
# MAGIC     delivery_hour INT NOT NULL,
# MAGIC     interval INT NOT NULL,
# MAGIC     zonal_price_capped_cad_per_mwh DECIMAL(18,2),
# MAGIC     loss_price_capped_cad_per_mwh DECIMAL(18,2),
# MAGIC     congestion_price_capped_cad_per_mwh DECIMAL(18,2),
# MAGIC     source_flag STRING,
# MAGIC     _source_name STRING NOT NULL,
# MAGIC     _source_file STRING NOT NULL,
# MAGIC     _source_url STRING NOT NULL,
# MAGIC     _source_hash STRING NOT NULL,
# MAGIC     _source_version STRING,
# MAGIC     _source_created_at STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP NOT NULL,
# MAGIC     _run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver.merge_into_silver(
    spark=spark,
    source_df=rt_silver_df,
    target_table="silver.price_realtime_5min",
    key_columns=[
        "delivery_date",
        "delivery_hour",
        "interval",
    ],
)

print("SRC-005 Silver MERGE completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_target_df = spark.table(
    "silver.price_realtime_5min"
)

duplicate_keys = (
    rt_target_df
    .groupBy(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_keys = (
    rt_silver_df
    .select(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .join(
        rt_target_df.select(
            "delivery_date",
            "delivery_hour",
            "interval",
        ),
        on=[
            "delivery_date",
            "delivery_hour",
            "interval",
        ],
        how="left_anti",
    )
    .count()
)

assert duplicate_keys == 0
assert missing_keys == 0

print(f"Silver rows: {rt_target_df.count():,}")
print(f"Duplicate business keys: {duplicate_keys}")
print(f"Missing incoming keys: {missing_keys}")
print("SRC-005 Silver validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Table for Data Quality Results

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS ops.dq_result (
# MAGIC     run_id STRING NOT NULL,
# MAGIC     source_name STRING NOT NULL,
# MAGIC     dataset_name STRING NOT NULL,
# MAGIC     rule_id STRING NOT NULL,
# MAGIC     rule_category STRING NOT NULL,
# MAGIC     severity STRING NOT NULL,
# MAGIC     status STRING NOT NULL,
# MAGIC     records_checked BIGINT,
# MAGIC     records_failed BIGINT,
# MAGIC     observed_value STRING,
# MAGIC     expected_value STRING,
# MAGIC     execution_timestamp TIMESTAMP NOT NULL,
# MAGIC     details STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS dq_rows
# MAGIC FROM ops.dq_result;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.dq as dq

dq = importlib.reload(dq)

dq_rows = []


def add_rule(
    *,
    registry_row,
    dataset_name,
    rule_id,
    rule_category,
    failed_count,
    records_checked,
    warn=False,
    expected_value="0 failures",
    details=None,
):
    severity = (
        "WARN"
        if warn and failed_count > 0
        else "FAIL"
        if not warn and failed_count > 0
        else "PASS"
    )

    dq_rows.append({
        "run_id": registry_row["run_id"],
        "source_name": registry_row["source_name"],
        "dataset_name": dataset_name,
        "rule_id": rule_id,
        "rule_category": rule_category,
        "severity": severity,
        "status": "COMPLETED",
        "records_checked": records_checked,
        "records_failed": failed_count,
        "observed_value": str(failed_count),
        "expected_value": expected_value,
        "details": details,
    })


def add_observation(
    *,
    registry_row,
    dataset_name,
    rule_id,
    observed_value,
    records_checked,
    details,
):
    dq_rows.append({
        "run_id": registry_row["run_id"],
        "source_name": registry_row["source_name"],
        "dataset_name": dataset_name,
        "rule_id": rule_id,
        "rule_category": "OBSERVATION",
        "severity": "PASS",
        "status": "COMPLETED",
        "records_checked": records_checked,
        "records_failed": None,
        "observed_value": str(observed_value),
        "expected_value": None,
        "details": details,
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_count = demand_typed_df.count()

for rule_id, failed_count in dq_results.items():
    add_rule(
        registry_row=demand_registry_row,
        dataset_name="silver.demand_hourly",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=demand_count,
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_physical_count = zonal_typed_df.count()

for rule_id, failed_count in zonal_dq_results["fail"].items():
    add_rule(
        registry_row=zonal_registry_row,
        dataset_name="silver.demand_zonal_hourly",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=(
            zonal_unpivoted_df.count()
            if rule_id == "duplicate_silver_key"
            else zonal_physical_count
        ),
    )

for rule_id, warn_count in zonal_dq_results["warn"].items():
    add_rule(
        registry_row=zonal_registry_row,
        dataset_name="silver.demand_zonal_hourly",
        rule_id=rule_id,
        rule_category="RECONCILIATION",
        failed_count=warn_count,
        records_checked=zonal_physical_count,
        warn=True,
        details=(
            "Source-level reconciliation observation. "
            "No root cause is assumed."
        ),
    )


add_rule(
    registry_row=zonal_registry_row,
    dataset_name="cross_source_demand_reconciliation",
    rule_id="src001_vs_src002_ontario_demand",
    rule_category="CROSS_SOURCE_RECONCILIATION",
    failed_count=mismatch_count,
    records_checked=overlap_count,
    warn=True,
    details=(
        "SRC-001 Ontario Demand compared with SRC-002 Ontario Demand. "
        "Values are preserved independently; mismatches are not overwritten."
    ),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_count = generation_typed_df.count()

for rule_id, failed_count in generation_dq_results["fail"].items():
    add_rule(
        registry_row=generation_registry_row,
        dataset_name="silver.generation_hourly",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=generation_count,
    )

add_observation(
    registry_row=generation_registry_row,
    dataset_name="silver.generation_hourly",
    rule_id="null_output_records",
    observed_value=(
        generation_dq_results["observations"]["null_output_records"]
    ),
    records_checked=generation_count,
    details=(
        "Output is nullable by source contract. "
        "Null values are preserved and are not converted to zero."
    ),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_count = da_typed_df.count()

for rule_id, failed_count in da_dq_results["fail"].items():
    add_rule(
        registry_row=da_registry_row,
        dataset_name="silver.price_day_ahead_hourly",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=da_count,
    )

for rule_id, observed_value in da_dq_results["observations"].items():
    add_observation(
        registry_row=da_registry_row,
        dataset_name="silver.price_day_ahead_hourly",
        rule_id=rule_id,
        observed_value=observed_value,
        records_checked=da_count,
        details=(
            "Source-permitted price observation. "
            "Negative prices/components and contractual nulls "
            "are not treated as failures."
        ),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_count = rt_typed_df.count()

for rule_id, failed_count in rt_dq_results["fail"].items():
    add_rule(
        registry_row=rt_registry_row,
        dataset_name="silver.price_realtime_5min",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=rt_count,
    )

for rule_id, warn_count in rt_dq_results["warn"].items():
    add_rule(
        registry_row=rt_registry_row,
        dataset_name="silver.price_realtime_5min",
        rule_id=rule_id,
        rule_category="SOURCE_STATE",
        failed_count=warn_count,
        records_checked=rt_count,
        warn=True,
        details=(
            "Partially populated interval slots are preserved "
            "rather than silently dropped."
        ),
    )

for rule_id, observed_value in rt_dq_results["observations"].items():
    add_observation(
        registry_row=rt_registry_row,
        dataset_name="silver.price_realtime_5min",
        rule_id=rule_id,
        observed_value=observed_value,
        records_checked=rt_count,
        details=(
            "Observed Real-Time interval population state."
        ),
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dq.upsert_dq_results(
    spark=spark,
    rows=dq_rows,
)

print(f"DQ results persisted: {len(dq_rows)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_dq_keys = (
    spark.table("ops.dq_result")
    .groupBy(
        "run_id",
        "source_name",
        "dataset_name",
        "rule_id",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_dq_keys == 0

cross_source_check = (
    spark.table("ops.dq_result")
    .filter(
        F.col("rule_id")
        == "src001_vs_src002_ontario_demand"
    )
    .orderBy(
        F.col("execution_timestamp").desc()
    )
    .limit(1)
    .collect()
)

assert len(cross_source_check) == 1
assert cross_source_check[0]["records_checked"] == 5712
assert cross_source_check[0]["records_failed"] == 1
assert cross_source_check[0]["severity"] == "WARN"

print("DQ persistence validation passed.")
print("Cross-source reconciliation: 5712 checked / 1 WARN mismatch.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


SILVER_TABLES = {
    "silver.demand_hourly": {
        "source": "ieso_hourly_demand",
        "keys": ["market_date", "hour_ending"],
    },
    "silver.demand_zonal_hourly": {
        "source": "ieso_hourly_zonal_demand",
        "keys": ["market_date", "hour_ending", "zone"],
    },
    "silver.generation_hourly": {
        "source": "ieso_generation_by_fuel_hourly",
        "keys": ["market_date", "hour_ending", "fuel_type"],
    },
    "silver.price_day_ahead_hourly": {
        "source": "ieso_day_ahead_ontario_zonal_price",
        "keys": ["market_date", "hour_ending"],
    },
    "silver.price_realtime_5min": {
        "source": "ieso_realtime_ontario_zonal_price",
        "keys": ["delivery_date", "delivery_hour", "interval"],
    },
}

registry_df = spark.table("ops.source_file_registry")
runs_df = spark.table("ops.etl_run")
dq_df = spark.table("ops.dq_result")

required_lineage = [
    "_source_name",
    "_source_file",
    "_source_url",
    "_source_hash",
    "_ingestion_timestamp",
    "_run_id",
]


for table_name, config in SILVER_TABLES.items():
    df = spark.table(table_name)

    row_count = df.count()
    assert row_count > 0, f"{table_name} is empty"

    duplicate_keys = (
        df.groupBy(*config["keys"])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_keys == 0, (
        f"Duplicate business keys found in {table_name}"
    )

    wrong_source = (
        df.filter(
            F.col("_source_name") != config["source"]
        ).count()
    )

    assert wrong_source == 0, (
        f"Unexpected source lineage in {table_name}"
    )

    for column in required_lineage:
        null_count = df.filter(
            F.col(column).isNull()
        ).count()

        assert null_count == 0, (
            f"Null required lineage {column} in {table_name}"
        )

    # Every Silver payload hash must reference a successful Bronze revision.
    orphan_hashes = (
        df.select(
            "_source_name",
            "_source_hash",
        )
        .distinct()
        .alias("s")
        .join(
            registry_df
            .filter(F.col("processing_status") == "SUCCESS")
            .select(
                F.col("source_name").alias("_source_name"),
                F.col("source_hash").alias("_source_hash"),
            )
            .distinct()
            .alias("r"),
            on=["_source_name", "_source_hash"],
            how="left_anti",
        )
        .count()
    )

    assert orphan_hashes == 0, (
        f"Silver lineage without successful Bronze payload: {table_name}"
    )

    # Every Silver run_id must exist in ETL history.
    orphan_runs = (
        df.select(
            F.col("_run_id").alias("run_id")
        )
        .distinct()
        .join(
            runs_df.select("run_id").distinct(),
            on="run_id",
            how="left_anti",
        )
        .count()
    )

    assert orphan_runs == 0, (
        f"Silver lineage without ETL run: {table_name}"
    )

    print(
        f"{table_name}: "
        f"{row_count:,} rows | "
        f"duplicates=0 | lineage=OK"
    )


# DQ audit.
dq_duplicate_keys = (
    dq_df
    .groupBy(
        "run_id",
        "source_name",
        "dataset_name",
        "rule_id",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert dq_duplicate_keys == 0

dq_errors = dq_df.filter(
    F.col("status") == "ERROR"
).count()

dq_failures = dq_df.filter(
    F.col("severity") == "FAIL"
).count()

assert dq_errors == 0
assert dq_failures == 0


# Validate the confirmed cross-source WARN.
cross_source_warn = (
    dq_df
    .filter(
        F.col("rule_id")
        == "src001_vs_src002_ontario_demand"
    )
    .orderBy(
        F.col("execution_timestamp").desc()
    )
    .limit(1)
    .collect()
)

assert len(cross_source_warn) == 1
assert cross_source_warn[0]["severity"] == "WARN"
assert cross_source_warn[0]["records_checked"] == 5712
assert cross_source_warn[0]["records_failed"] == 1


print("\nSilver framework audit passed.")
print(f"DQ results: {dq_df.count():,}")
print(f"DQ ERROR results: {dq_errors}")
print(f"DQ FAIL results: {dq_failures}")
print("Cross-source demand reconciliation: 5,712 checked / 1 WARN.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

import builtin.gridpulse.parsers.demand as demand_parser
import builtin.gridpulse.dq as dq


test_schema = StructType([
    StructField("date_raw", StringType(), True),
    StructField("hour_raw", StringType(), True),
    StructField("market_demand_raw", StringType(), True),
    StructField("ontario_demand_raw", StringType(), True),
    StructField("_source_row_number", LongType(), False),
])


# Two rows deliberately share the same business key.
invalid_raw_df = spark.createDataFrame(
    [
        ("2026-01-01", "1", "18000", "17500", 1),
        ("2026-01-01", "1", "18100", "17600", 2),
    ],
    schema=test_schema,
)

invalid_typed_df = demand_parser.type_hourly_demand(
    invalid_raw_df
)

rows_before = spark.table(
    "silver.demand_hourly"
).count()

failure_detected = False

try:
    dq.validate_hourly_demand(
        invalid_typed_df
    )

except ValueError as exc:
    failure_detected = True

    assert "duplicate_business_key" in str(exc)

    print("Expected Silver DQ failure detected.")
    print(str(exc))


assert failure_detected

rows_after = spark.table(
    "silver.demand_hourly"
).count()

assert rows_after == rows_before

print(f"Silver rows before test: {rows_before:,}")
print(f"Silver rows after test:  {rows_after:,}")
print("Pre-MERGE grain protection validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

import builtin.gridpulse.parsers.demand as demand_parser
import builtin.gridpulse.dq as dq


test_schema = StructType([
    StructField("date_raw", StringType(), True),
    StructField("hour_raw", StringType(), True),
    StructField("market_demand_raw", StringType(), True),
    StructField("ontario_demand_raw", StringType(), True),
    StructField("_source_row_number", LongType(), False),
])


invalid_raw_df = spark.createDataFrame(
    [
        (
            "2026-01-01",
            "1",
            "NOT_A_NUMBER",
            "17500",
            1,
        )
    ],
    schema=test_schema,
)

invalid_typed_df = demand_parser.type_hourly_demand(
    invalid_raw_df
)

rows_before = spark.table(
    "silver.demand_hourly"
).count()

failure_detected = False

try:
    dq.validate_hourly_demand(
        invalid_typed_df
    )

except ValueError as exc:
    failure_detected = True

    assert "market_demand_numeric" in str(exc)

    print("Expected malformed-value DQ failure detected.")
    print(str(exc))


assert failure_detected

rows_after = spark.table(
    "silver.demand_hourly"
).count()

assert rows_after == rows_before

print(f"Silver rows before test: {rows_before:,}")
print(f"Silver rows after test:  {rows_after:,}")
print("Malformed required value protection passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

import builtin.gridpulse.parsers.generation as generation_parser
import builtin.gridpulse.dq as dq


test_schema = StructType([
    StructField("date_raw", StringType(), True),
    StructField("hour_raw", StringType(), True),
    StructField("fuel_type_raw", StringType(), True),
    StructField("output_raw", StringType(), True),
    StructField("output_quality_raw", StringType(), True),
    StructField("_source_record_number", LongType(), False),
])


valid_null_raw_df = spark.createDataFrame(
    [
        (
            "2026-07-15",
            "1",
            "CONTROL ACTIONS",
            None,
            "-1",
            1,
        )
    ],
    schema=test_schema,
)

valid_null_typed_df = generation_parser.type_generation_hourly(
    valid_null_raw_df
)

dq_result = dq.validate_generation_hourly(
    valid_null_typed_df
)

row = valid_null_typed_df.collect()[0]

assert row["output_mwh"] is None
assert dq_result["fail"]["malformed_output"] == 0
assert dq_result["observations"]["null_output_records"] == 1

print("Source-permitted null accepted.")
print(f"output_mwh: {row['output_mwh']}")
print(
    "Null output observations:",
    dq_result["observations"]["null_output_records"],
)
print("Source-permitted null preservation test passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

import builtin.gridpulse.parsers.price_realtime as rt_parser
import builtin.gridpulse.dq as dq


test_schema = StructType([
    StructField("delivery_date_raw", StringType(), True),
    StructField("delivery_hour_raw", StringType(), True),
    StructField("interval_raw", StringType(), True),
    StructField("flag_raw", StringType(), True),
    StructField("lmp_cap_raw", StringType(), True),
    StructField("loss_price_cap_raw", StringType(), True),
    StructField("cong_price_cap_raw", StringType(), True),
    StructField("created_at_raw", StringType(), True),
    StructField("_source_record_number", LongType(), False),
])


# Build all 12 slots so the contract-level interval check also passes.
test_rows = []

for interval in range(1, 13):
    if interval == 12:
        test_rows.append(
            (
                "2026-08-25",
                "18",
                str(interval),
                None,
                None,
                None,
                None,
                "2026-08-25T18:55:00",
                interval,
            )
        )
    else:
        test_rows.append(
            (
                "2026-08-25",
                "18",
                str(interval),
                None,
                "50.00",
                "0.10",
                "-0.20",
                "2026-08-25T18:55:00",
                interval,
            )
        )

rt_test_raw_df = spark.createDataFrame(
    test_rows,
    schema=test_schema,
)

rt_test_typed_df = rt_parser.type_realtime_price(
    rt_test_raw_df
)

rt_test_dq = dq.validate_realtime_price(
    rt_test_typed_df
)

empty_slot = (
    rt_test_typed_df
    .filter("interval = 12")
    .collect()[0]
)

assert empty_slot["zonal_price_capped_cad_per_mwh"] is None
assert empty_slot["loss_price_capped_cad_per_mwh"] is None
assert empty_slot["congestion_price_capped_cad_per_mwh"] is None

assert rt_test_dq["fail"]["malformed_populated_price"] == 0
assert rt_test_dq["observations"]["fully_empty_intervals"] == 1
assert rt_test_dq["warn"]["partially_populated_intervals"] == 0

print("Fully empty RT interval accepted.")
print("Fully empty intervals: 1")
print("Partially populated intervals: 0")
print("RT empty-slot preservation test passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

import builtin.gridpulse.parsers.price_realtime as rt_parser
import builtin.gridpulse.dq as dq


test_schema = StructType([
    StructField("delivery_date_raw", StringType(), True),
    StructField("delivery_hour_raw", StringType(), True),
    StructField("interval_raw", StringType(), True),
    StructField("flag_raw", StringType(), True),
    StructField("lmp_cap_raw", StringType(), True),
    StructField("loss_price_cap_raw", StringType(), True),
    StructField("cong_price_cap_raw", StringType(), True),
    StructField("created_at_raw", StringType(), True),
    StructField("_source_record_number", LongType(), False),
])


test_rows = []

for interval in range(1, 13):
    if interval == 12:
        # Deliberately partial: one populated component, two missing.
        test_rows.append(
            (
                "2026-08-25",
                "18",
                str(interval),
                None,
                "50.00",
                None,
                None,
                "2026-08-25T18:55:00",
                interval,
            )
        )
    else:
        test_rows.append(
            (
                "2026-08-25",
                "18",
                str(interval),
                None,
                "50.00",
                "0.10",
                "-0.20",
                "2026-08-25T18:55:00",
                interval,
            )
        )

partial_raw_df = spark.createDataFrame(
    test_rows,
    schema=test_schema,
)

partial_typed_df = rt_parser.type_realtime_price(
    partial_raw_df
)

partial_dq = dq.validate_realtime_price(
    partial_typed_df
)

partial_slot = (
    partial_typed_df
    .filter("interval = 12")
    .collect()[0]
)

assert partial_slot["zonal_price_capped_cad_per_mwh"] is not None
assert partial_slot["loss_price_capped_cad_per_mwh"] is None
assert partial_slot["congestion_price_capped_cad_per_mwh"] is None

assert partial_dq["fail"]["malformed_populated_price"] == 0
assert partial_dq["warn"]["partially_populated_intervals"] == 1
assert partial_dq["observations"]["fully_empty_intervals"] == 0

print("Partially populated RT interval preserved.")
print("Partially populated intervals: 1")
print("RT partial-slot WARN test passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Final Audit Phase 2

# CELL ********************

import os

from pyspark.sql import functions as F


EXPECTED_SILVER_COUNTS = {
    "silver.demand_hourly": 5713,
    "silver.demand_zonal_hourly": 57120,
    "silver.generation_hourly": 40008,
    "silver.price_day_ahead_hourly": 24,
    "silver.price_realtime_5min": 12,
}

for table_name, expected_count in EXPECTED_SILVER_COUNTS.items():
    actual_count = spark.table(table_name).count()

    assert actual_count == expected_count, (
        f"{table_name}: expected {expected_count}, found {actual_count}"
    )

    print(f"{table_name}: {actual_count:,} rows | unchanged")


registry_df = spark.table("ops.source_file_registry")
runs_df = spark.table("ops.etl_run")
dq_df = spark.table("ops.dq_result")


assert registry_df.filter(
    F.col("processing_status") == "PENDING"
).count() == 0

assert runs_df.filter(
    F.col("status") == "RUNNING"
).count() == 0

assert dq_df.count() == 38

assert dq_df.filter(
    F.col("status") == "ERROR"
).count() == 0

assert dq_df.filter(
    F.col("severity") == "FAIL"
).count() == 0


# Recovery test table must be gone.
assert not spark.catalog.tableExists(
    "ops.source_file_registry_recovery_test"
)


# No synthetic test files should remain.
test_root = "/lakehouse/default/Files/bronze/_tests"

remaining_test_files = []

if os.path.exists(test_root):
    for root, _, files in os.walk(test_root):
        for file_name in files:
            remaining_test_files.append(
                os.path.join(root, file_name)
            )

assert len(remaining_test_files) == 0, (
    f"Synthetic Bronze test files remain: {remaining_test_files}"
)


cross_source_warn = (
    dq_df
    .filter(
        F.col("rule_id")
        == "src001_vs_src002_ontario_demand"
    )
    .collect()
)

assert len(cross_source_warn) == 1
assert cross_source_warn[0]["severity"] == "WARN"
assert cross_source_warn[0]["records_checked"] == 5712
assert cross_source_warn[0]["records_failed"] == 1


print("\nDAY 2 regression audit passed.")
print("Production Bronze/Silver state unchanged by tests.")
print("DQ results: 38 | ERROR: 0 | FAIL: 0")
print("Cross-source reconciliation: 5,712 checked / 1 WARN")
print("Synthetic test artifacts: 0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
