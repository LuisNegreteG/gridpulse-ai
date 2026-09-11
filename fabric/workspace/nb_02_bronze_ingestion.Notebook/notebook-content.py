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

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS ops.source_file_registry (
# MAGIC     source_name STRING NOT NULL,
# MAGIC     logical_source_key STRING NOT NULL,
# MAGIC     source_url STRING NOT NULL,
# MAGIC     source_file STRING NOT NULL,
# MAGIC     source_version STRING,
# MAGIC     file_size BIGINT NOT NULL,
# MAGIC     source_hash STRING NOT NULL,
# MAGIC     source_created_at STRING,
# MAGIC     bronze_path STRING,
# MAGIC     first_seen_timestamp TIMESTAMP NOT NULL,
# MAGIC     last_seen_timestamp TIMESTAMP NOT NULL,
# MAGIC     processing_status STRING NOT NULL,
# MAGIC     run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE ops.source_file_registry;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS registry_rows
# MAGIC FROM ops.source_file_registry;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS ops.etl_run (
# MAGIC     run_id STRING NOT NULL,
# MAGIC     pipeline_name STRING NOT NULL,
# MAGIC     source_name STRING NOT NULL,
# MAGIC     start_timestamp TIMESTAMP NOT NULL,
# MAGIC     end_timestamp TIMESTAMP,
# MAGIC     status STRING NOT NULL,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     records_rejected BIGINT,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS run_rows
# MAGIC FROM ops.etl_run;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.sources import SOURCE_CONFIGS

expected_sources = {
    "ieso_hourly_demand",
    "ieso_hourly_zonal_demand",
    "ieso_generation_by_fuel_hourly",
    "ieso_day_ahead_ontario_zonal_price",
    "ieso_realtime_ontario_zonal_price",
}

assert set(SOURCE_CONFIGS.keys()) == expected_sources
assert len(SOURCE_CONFIGS) == 5

for source_name, config in SOURCE_CONFIGS.items():
    assert config["source_name"] == source_name
    assert config["bronze_base_path"].startswith("Files/bronze/ieso/")
    assert config["payload_format"] in {"csv", "xml"}

print("Source configuration validation passed.")
print(f"Configured sources: {len(SOURCE_CONFIGS)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.identity import build_logical_source_key


test_keys = {
    "demand": build_logical_source_key(
        "ieso_hourly_demand",
        year=2026,
    ),
    "zonal": build_logical_source_key(
        "ieso_hourly_zonal_demand",
        year=2026,
    ),
    "generation": build_logical_source_key(
        "ieso_generation_by_fuel_hourly",
        year=2026,
    ),
    "day_ahead": build_logical_source_key(
        "ieso_day_ahead_ontario_zonal_price",
        delivery_date="2026-08-16",
    ),
    "realtime": build_logical_source_key(
        "ieso_realtime_ontario_zonal_price",
        source_file="PUB_RealtimeOntarioZonalPrice.xml",
    ),
}


assert test_keys["demand"] == (
    "ieso_hourly_demand|year=2026"
)

assert test_keys["zonal"] == (
    "ieso_hourly_zonal_demand|year=2026"
)

assert test_keys["generation"] == (
    "ieso_generation_by_fuel_hourly|year=2026"
)

assert test_keys["day_ahead"] == (
    "ieso_day_ahead_ontario_zonal_price|delivery_date=2026-08-16"
)

assert test_keys["realtime"] == (
    "ieso_realtime_ontario_zonal_price"
    "|alias=PUB_RealtimeOntarioZonalPrice.xml"
)

for name, key in test_keys.items():
    print(f"{name}: {key}")

print("\nLogical source key validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.bronze import compute_sha256, build_bronze_path


test_payload = b"gridpulse-bronze-test"

test_hash = compute_sha256(test_payload)

test_path = build_bronze_path(
    bronze_base_path="Files/bronze/ieso/demand",
    source_hash=test_hash,
    source_file="PUB_Demand_2026.csv",
)

assert len(test_hash) == 64

assert test_path == (
    f"Files/bronze/ieso/demand/"
    f"{test_hash}/"
    f"PUB_Demand_2026.csv"
)

print(f"SHA-256: {test_hash}")
print(f"Bronze path: {test_path}")
print("Bronze identity validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.registry import classify_payload


test_result = classify_payload(
    spark=spark,
    logical_source_key="ieso_hourly_demand|year=2026",
    source_hash="a" * 64,
)

assert test_result == "NEW"

print(f"Registry decision: {test_result}")
print("Registry lookup validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

print("Spark session timezone: UTC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.registry as registry

registry = importlib.reload(registry)

assert hasattr(registry, "register_payload_pending")
assert hasattr(registry, "finalize_payload_success")
assert hasattr(registry, "finalize_payload_failed")

print("Registry module reloaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.registry import (
    classify_payload,
    register_payload_pending,
    finalize_payload_success,
    finalize_payload_failed,
)

registry_rows = spark.table("ops.source_file_registry").count()

assert registry_rows == 0

print("Registry state functions imported successfully.")
print(f"Production registry rows: {registry_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.run as run_module

run_module = importlib.reload(run_module)

test_run_id = run_module.generate_run_id()

assert isinstance(test_run_id, str)
assert len(test_run_id) == 36

print(f"Generated run_id: {test_run_id}")
print("ETL run module validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.acquisition as acquisition

acquisition = importlib.reload(acquisition)

test_payload = acquisition.build_acquired_payload(
    source_name="ieso_hourly_demand",
    source_url="https://example.invalid/test",
    source_file="PUB_Demand_2026.csv",
    payload_bytes=b"test-payload",
)

assert test_payload.source_name == "ieso_hourly_demand"
assert test_payload.source_file == "PUB_Demand_2026.csv"
assert test_payload.payload_bytes == b"test-payload"
assert test_payload.retrieved_at_utc.tzinfo is not None

print("Acquisition contract validation passed.")
print(test_payload)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.acquisition as acquisition
import builtin.gridpulse.bronze as bronze
import builtin.gridpulse.identity as identity

acquisition = importlib.reload(acquisition)
bronze = importlib.reload(bronze)
identity = importlib.reload(identity)


SOURCE_NAME = "ieso_hourly_demand"
SOURCE_FILE = "PUB_Demand_2026.csv"
SOURCE_URL = (
    "https://reports-public.ieso.ca/public/Demand/"
    "PUB_Demand_2026.csv"
)

payload = acquisition.acquire_http_payload(
    source_name=SOURCE_NAME,
    source_url=SOURCE_URL,
    source_file=SOURCE_FILE,
)

source_hash = bronze.compute_sha256(payload.payload_bytes)

logical_source_key = identity.build_logical_source_key(
    SOURCE_NAME,
    year=2026,
)


assert payload.source_name == SOURCE_NAME
assert payload.source_file == SOURCE_FILE
assert len(payload.payload_bytes) > 0
assert len(source_hash) == 64

print(f"Source: {payload.source_name}")
print(f"File: {payload.source_file}")
print(f"Bytes retrieved: {len(payload.payload_bytes):,}")
print(f"SHA-256: {source_hash}")
print(f"Logical source key: {logical_source_key}")
print("Real acquisition validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.registry as registry
import builtin.gridpulse.bronze as bronze
import builtin.gridpulse.sources as sources

registry = importlib.reload(registry)
bronze = importlib.reload(bronze)
sources = importlib.reload(sources)


classification = registry.classify_payload(
    spark=spark,
    logical_source_key=logical_source_key,
    source_hash=source_hash,
)

bronze_base_path = (
    sources.SOURCE_CONFIGS[SOURCE_NAME]["bronze_base_path"]
)

bronze_path = bronze.build_bronze_path(
    bronze_base_path=bronze_base_path,
    source_hash=source_hash,
    source_file=SOURCE_FILE,
)


assert classification == "NEW"
assert bronze_path.startswith(
    "Files/bronze/ieso/demand/"
)
assert source_hash in bronze_path
assert bronze_path.endswith(
    "/PUB_Demand_2026.csv"
)

print(f"Classification: {classification}")
print(f"Bronze path: {bronze_path}")
print("Payload classification validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.run as run_module
import builtin.gridpulse.registry as registry

run_module = importlib.reload(run_module)
registry = importlib.reload(registry)


run_id = run_module.generate_run_id()

payload_metadata = {
    "source_name": payload.source_name,
    "logical_source_key": logical_source_key,
    "source_url": payload.source_url,
    "source_file": payload.source_file,
    "source_version": payload.source_version,
    "file_size": len(payload.payload_bytes),
    "source_hash": source_hash,
    "source_created_at": payload.source_created_at,
    "run_id": run_id,
}


try:
    run_module.start_etl_run(
        spark=spark,
        run_id=run_id,
        pipeline_name="bronze_ingestion",
        source_name=SOURCE_NAME,
    )

    registry.register_payload_pending(
        spark=spark,
        payload_metadata=payload_metadata,
    )

except Exception as exc:
    run_module.finalize_etl_run(
        spark=spark,
        run_id=run_id,
        status="FAILED",
        error_message=str(exc),
    )
    raise


print(f"Run ID: {run_id}")
print("Payload registered as PENDING.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

run_row = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == run_id)
    .collect()
)

registry_row = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("logical_source_key") == logical_source_key)
        & (F.col("source_hash") == source_hash)
    )
    .collect()
)

assert len(run_row) == 1
assert run_row[0]["status"] == "RUNNING"

assert len(registry_row) == 1
assert registry_row[0]["processing_status"] == "PENDING"
assert registry_row[0]["bronze_path"] is None

print(f"ETL run status: {run_row[0]['status']}")
print(f"Registry status: {registry_row[0]['processing_status']}")
print("Pre-Bronze state validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.bronze as bronze
import builtin.gridpulse.registry as registry
import builtin.gridpulse.run as run_module

bronze = importlib.reload(bronze)
registry = importlib.reload(registry)
run_module = importlib.reload(run_module)


try:
    write_result = bronze.persist_bronze_payload(
        payload_bytes=payload.payload_bytes,
        bronze_path=bronze_path,
        expected_hash=source_hash,
    )

    registry.finalize_payload_success(
        spark=spark,
        logical_source_key=logical_source_key,
        source_hash=source_hash,
        bronze_path=bronze_path,
    )

    run_module.finalize_etl_run(
        spark=spark,
        run_id=run_id,
        status="SUCCESS",
    )

except Exception as exc:
    registry.finalize_payload_failed(
        spark=spark,
        logical_source_key=logical_source_key,
        source_hash=source_hash,
    )

    run_module.finalize_etl_run(
        spark=spark,
        run_id=run_id,
        status="FAILED",
        error_message=str(exc),
    )

    raise


print(f"Bronze path: {write_result['bronze_path']}")
print(f"Persisted bytes: {write_result['file_size']:,}")
print(f"Verified SHA-256: {write_result['source_hash']}")
print(f"Reused existing file: {write_result['reused_existing']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

registry_check = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("logical_source_key") == logical_source_key)
        & (F.col("source_hash") == source_hash)
    )
    .collect()
)

run_check = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == run_id)
    .collect()
)

assert len(registry_check) == 1
assert registry_check[0]["processing_status"] == "SUCCESS"
assert registry_check[0]["bronze_path"] == bronze_path

assert len(run_check) == 1
assert run_check[0]["status"] == "SUCCESS"
assert run_check[0]["end_timestamp"] is not None

print("Registry status: SUCCESS")
print("ETL run status: SUCCESS")
print("Bronze persistence and integrity validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib

import builtin.gridpulse.registry as registry
import builtin.gridpulse.run as run_module

registry = importlib.reload(registry)
run_module = importlib.reload(run_module)


second_run_id = run_module.generate_run_id()

run_module.start_etl_run(
    spark=spark,
    run_id=second_run_id,
    pipeline_name="bronze_ingestion",
    source_name=SOURCE_NAME,
)

classification = registry.classify_payload(
    spark=spark,
    logical_source_key=logical_source_key,
    source_hash=source_hash,
)

assert classification == "UNCHANGED"

registry.touch_payload_seen(
    spark=spark,
    logical_source_key=logical_source_key,
    source_hash=source_hash,
)

run_module.finalize_etl_run(
    spark=spark,
    run_id=second_run_id,
    status="SUCCESS",
)

print(f"Classification: {classification}")
print(f"Run ID: {second_run_id}")
print("No Bronze write executed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

registry_rows = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("logical_source_key") == logical_source_key)
        & (F.col("source_hash") == source_hash)
    )
    .collect()
)

second_run_rows = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == second_run_id)
    .collect()
)

assert len(registry_rows) == 1
assert registry_rows[0]["processing_status"] == "SUCCESS"

assert len(second_run_rows) == 1
assert second_run_rows[0]["status"] == "SUCCESS"

assert registry_rows[0]["run_id"] == run_id

print("Registry payload rows: 1")
print("Original registry run_id preserved.")
print("Second ETL run: SUCCESS")
print("Idempotency validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.registry as registry

registry = importlib.reload(registry)

print("Registry module reloaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

test_existing_hash = "a" * 64
test_new_hash = "b" * 64

assert test_existing_hash != test_new_hash

test_registry_df = spark.createDataFrame([
    Row(
        logical_source_key="ieso_hourly_demand|year=2026",
        source_hash=test_existing_hash,
        processing_status="SUCCESS",
        bronze_path=(
            "Files/bronze/ieso/demand/"
            f"{test_existing_hash}/PUB_Demand_2026.csv"
        ),
    )
])

test_registry_df.createOrReplaceTempView(
    "tmp_source_file_registry_revision_test"
)

classification = registry.classify_payload(
    spark=spark,
    logical_source_key="ieso_hourly_demand|year=2026",
    source_hash=test_new_hash,
    table_name="tmp_source_file_registry_revision_test",
)

assert classification == "REVISED"

print(f"Classification: {classification}")
print("REVISED classification validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.dropTempView(
    "tmp_source_file_registry_revision_test"
)

print("Temporary revision test cleaned up.")

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
)

test_registry_schema = StructType([
    StructField("logical_source_key", StringType(), False),
    StructField("source_hash", StringType(), False),
    StructField("processing_status", StringType(), False),
    StructField("bronze_path", StringType(), True),
])

test_hash = "c" * 64
test_key = "ieso_hourly_demand|year=2026"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_registry_df = spark.createDataFrame(
    [
        (
            test_key,
            test_hash,
            "FAILED",
            None,
        )
    ],
    schema=test_registry_schema,
)

failed_registry_df.createOrReplaceTempView(
    "tmp_registry_failed_test"
)

classification = registry.classify_payload(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    table_name="tmp_registry_failed_test",
)

assert classification == "RECOVER"

print(f"FAILED classification: {classification}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pending_registry_df = spark.createDataFrame(
    [
        (
            test_key,
            test_hash,
            "PENDING",
            None,
        )
    ],
    schema=test_registry_schema,
)

pending_registry_df.createOrReplaceTempView(
    "tmp_registry_pending_test"
)

classification = registry.classify_payload(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    table_name="tmp_registry_pending_test",
)

assert classification == "RECOVER"

print(f"PENDING classification: {classification}")
print("RECOVER validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.dropTempView("tmp_registry_failed_test")
spark.catalog.dropTempView("tmp_registry_pending_test")

print("Temporary recovery tests cleaned up.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

orchestration = importlib.reload(orchestration)

result = orchestration.ingest_http_source(
    spark=spark,
    source_name="ieso_hourly_demand",
    source_url=(
        "https://reports-public.ieso.ca/public/Demand/"
        "PUB_Demand_2026.csv"
    ),
    source_file="PUB_Demand_2026.csv",
    logical_key_kwargs={
        "year": 2026,
    },
)

print(f"Run ID: {result['run_id']}")
print(f"Classification: {result['classification']}")
print(f"Bronze write: {result['bronze_write']}")
print(f"Reused existing file: {result['reused_existing_file']}")
print(f"Bronze path: {result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

run_validation = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == result["run_id"])
    .collect()
)

assert len(run_validation) == 1
assert run_validation[0]["status"] == "SUCCESS"
assert run_validation[0]["end_timestamp"] is not None

print("Orchestrated Bronze ingestion validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

orchestration = importlib.reload(orchestration)

zonal_result = orchestration.ingest_http_source(
    spark=spark,
    source_name="ieso_hourly_zonal_demand",
    source_url=(
        "https://reports-public.ieso.ca/public/DemandZonal/"
        "PUB_DemandZonal_2026.csv"
    ),
    source_file="PUB_DemandZonal_2026.csv",
    logical_key_kwargs={
        "year": 2026,
    },
)

print(f"Run ID: {zonal_result['run_id']}")
print(f"Classification: {zonal_result['classification']}")
print(f"Bronze write: {zonal_result['bronze_write']}")
print(f"Bronze path: {zonal_result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

zonal_run = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == zonal_result["run_id"])
    .collect()
)

zonal_registry = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("source_name") == "ieso_hourly_zonal_demand")
        & (F.col("source_hash") == zonal_result["source_hash"])
    )
    .collect()
)

assert len(zonal_run) == 1
assert zonal_run[0]["status"] == "SUCCESS"

assert len(zonal_registry) == 1
assert zonal_registry[0]["processing_status"] == "SUCCESS"
assert zonal_registry[0]["bronze_path"] == zonal_result["bronze_path"]

print("SRC-002 Bronze ingestion validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

orchestration = importlib.reload(orchestration)

generation_result = orchestration.ingest_http_source(
    spark=spark,
    source_name="ieso_generation_by_fuel_hourly",
    source_url=(
        "https://reports-public.ieso.ca/public/"
        "GenOutputbyFuelHourly/"
        "PUB_GenOutputbyFuelHourly_2026.xml"
    ),
    source_file="PUB_GenOutputbyFuelHourly_2026.xml",
    logical_key_kwargs={
        "year": 2026,
    },
)

print(f"Run ID: {generation_result['run_id']}")
print(f"Classification: {generation_result['classification']}")
print(f"Bronze write: {generation_result['bronze_write']}")
print(f"Bronze path: {generation_result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

generation_run = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == generation_result["run_id"])
    .collect()
)

generation_registry = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("source_name") == "ieso_generation_by_fuel_hourly")
        & (F.col("source_hash") == generation_result["source_hash"])
    )
    .collect()
)

assert len(generation_run) == 1
assert generation_run[0]["status"] == "SUCCESS"

assert len(generation_registry) == 1
assert generation_registry[0]["processing_status"] == "SUCCESS"
assert generation_registry[0]["bronze_path"] == generation_result["bronze_path"]

print("SRC-003 Bronze ingestion validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

orchestration = importlib.reload(orchestration)

da_result = orchestration.ingest_http_source(
    spark=spark,
    source_name="ieso_day_ahead_ontario_zonal_price",
    source_url=(
        "https://reports-public.ieso.ca/public/"
        "DAHourlyOntarioZonalPrice/"
        "PUB_DAHourlyOntarioZonalPrice_20260816.xml"
    ),
    source_file="PUB_DAHourlyOntarioZonalPrice_20260816.xml",
    logical_key_kwargs={
        "delivery_date": "2026-08-16",
    },
)

print(f"Run ID: {da_result['run_id']}")
print(f"Classification: {da_result['classification']}")
print(f"Bronze write: {da_result['bronze_write']}")
print(f"Bronze path: {da_result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

da_registry = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("source_name") == "ieso_day_ahead_ontario_zonal_price")
        & (F.col("source_hash") == da_result["source_hash"])
    )
    .collect()
)

da_run = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == da_result["run_id"])
    .collect()
)

assert len(da_registry) == 1
assert da_registry[0]["processing_status"] == "SUCCESS"

assert (
    da_registry[0]["logical_source_key"]
    == "ieso_day_ahead_ontario_zonal_price|delivery_date=2026-08-16"
)

assert len(da_run) == 1
assert da_run[0]["status"] == "SUCCESS"

print("SRC-004 Bronze ingestion validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.orchestration as orchestration

orchestration = importlib.reload(orchestration)

rt_result = orchestration.ingest_http_source(
    spark=spark,
    source_name="ieso_realtime_ontario_zonal_price",
    source_url=(
        "https://reports-public.ieso.ca/public/"
        "RealtimeOntarioZonalPrice/"
        "PUB_RealtimeOntarioZonalPrice.xml"
    ),
    source_file="PUB_RealtimeOntarioZonalPrice.xml",
    logical_key_kwargs={
        "source_file": "PUB_RealtimeOntarioZonalPrice.xml",
    },
)

print(f"Run ID: {rt_result['run_id']}")
print(f"Classification: {rt_result['classification']}")
print(f"Bronze write: {rt_result['bronze_write']}")
print(f"Bronze path: {rt_result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rt_registry = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("source_name") == "ieso_realtime_ontario_zonal_price")
        & (F.col("source_hash") == rt_result["source_hash"])
    )
    .collect()
)

rt_run = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == rt_result["run_id"])
    .collect()
)

assert len(rt_registry) == 1
assert rt_registry[0]["processing_status"] == "SUCCESS"

assert (
    rt_registry[0]["logical_source_key"]
    == (
        "ieso_realtime_ontario_zonal_price"
        "|alias=PUB_RealtimeOntarioZonalPrice.xml"
    )
)

assert len(rt_run) == 1
assert rt_run[0]["status"] == "SUCCESS"

print("SRC-005 Bronze ingestion validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Final Validation

# CELL ********************

import os

from pyspark.sql import functions as F
from builtin.gridpulse.bronze import compute_file_sha256


EXPECTED_SOURCES = {
    "ieso_hourly_demand",
    "ieso_hourly_zonal_demand",
    "ieso_generation_by_fuel_hourly",
    "ieso_day_ahead_ontario_zonal_price",
    "ieso_realtime_ontario_zonal_price",
}

registry_df = spark.table("ops.source_file_registry")
runs_df = spark.table("ops.etl_run")


# 1. Validate registry statuses.
invalid_registry_statuses = (
    registry_df
    .filter(~F.col("processing_status").isin("PENDING", "SUCCESS", "FAILED"))
    .count()
)

assert invalid_registry_statuses == 0


# 2. No ingestion should remain PENDING after Bronze processing is complete.
pending_payloads = (
    registry_df
    .filter(F.col("processing_status") == "PENDING")
    .count()
)

assert pending_payloads == 0


# 3. Validate registry revision uniqueness.
duplicate_revisions = (
    registry_df
    .groupBy("logical_source_key", "source_hash")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_revisions == 0


# 4. Every configured source must have at least one successful Bronze payload.
successful_sources = {
    row["source_name"]
    for row in (
        registry_df
        .filter(F.col("processing_status") == "SUCCESS")
        .select("source_name")
        .distinct()
        .collect()
    )
}

assert EXPECTED_SOURCES.issubset(successful_sources)


# 5. Validate ETL run states and run_id uniqueness.
invalid_run_statuses = (
    runs_df
    .filter(~F.col("status").isin("RUNNING", "SUCCESS", "FAILED"))
    .count()
)

assert invalid_run_statuses == 0

running_runs = (
    runs_df
    .filter(F.col("status") == "RUNNING")
    .count()
)

assert running_runs == 0

assert runs_df.count() == runs_df.select("run_id").distinct().count()


# 6. Every registry run_id must exist in ops.etl_run.
orphan_registry_runs = (
    registry_df.alias("r")
    .join(
        runs_df.select("run_id").alias("e"),
        on="run_id",
        how="left_anti",
    )
    .count()
)

assert orphan_registry_runs == 0


# 7. Verify every successful Bronze payload physically exists
#    and matches its registered size and SHA-256.
success_rows = (
    registry_df
    .filter(F.col("processing_status") == "SUCCESS")
    .collect()
)

for row in success_rows:
    assert row["bronze_path"] is not None
    assert row["bronze_path"].startswith("Files/bronze/ieso/")
    assert row["bronze_path"].endswith(f"/{row['source_file']}")

    local_path = f"/lakehouse/default/{row['bronze_path']}"

    assert os.path.isfile(local_path), (
        f"Missing Bronze file: {row['bronze_path']}"
    )

    persisted_size = os.path.getsize(local_path)

    assert persisted_size == row["file_size"], (
        f"File-size mismatch: {row['bronze_path']}"
    )

    persisted_hash = compute_file_sha256(local_path)

    assert persisted_hash == row["source_hash"], (
        f"SHA-256 mismatch: {row['bronze_path']}"
    )


print("Bronze framework audit passed.")
print(f"Successful Bronze revisions verified: {len(success_rows)}")
print(f"Sources with successful Bronze payloads: {len(successful_sources)}")
print(f"ETL runs recorded: {runs_df.count()}")
print(f"Pending payloads: {pending_payloads}")
print(f"Running ETL runs: {running_runs}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

from builtin.gridpulse.bronze import (
    compute_sha256,
    build_bronze_path,
    persist_bronze_payload,
)


test_payload = b"gridpulse-integrity-failure-test"

real_hash = compute_sha256(test_payload)
wrong_hash = "f" * 64

test_path = build_bronze_path(
    bronze_base_path="Files/bronze/_tests/integrity",
    source_hash=real_hash,
    source_file="integrity_test.bin",
)

local_path = f"/lakehouse/default/{test_path}"

failure_detected = False

try:
    persist_bronze_payload(
        payload_bytes=test_payload,
        bronze_path=test_path,
        expected_hash=wrong_hash,
    )

except RuntimeError as exc:
    failure_detected = True

    assert "SHA-256" in str(exc)

    print("Expected integrity failure detected.")
    print(str(exc))

finally:
    # Remove the synthetic test artifact.
    if os.path.exists(local_path):
        os.remove(local_path)

    parent_dir = os.path.dirname(local_path)

    if os.path.isdir(parent_dir) and not os.listdir(parent_dir):
        os.rmdir(parent_dir)


assert failure_detected

print("Bronze integrity failure test passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS ops.source_file_registry_recovery_test;
# MAGIC 
# MAGIC CREATE TABLE ops.source_file_registry_recovery_test (
# MAGIC     source_name STRING NOT NULL,
# MAGIC     logical_source_key STRING NOT NULL,
# MAGIC     source_url STRING NOT NULL,
# MAGIC     source_file STRING NOT NULL,
# MAGIC     source_version STRING,
# MAGIC     file_size BIGINT NOT NULL,
# MAGIC     source_hash STRING NOT NULL,
# MAGIC     source_created_at STRING,
# MAGIC     bronze_path STRING,
# MAGIC     first_seen_timestamp TIMESTAMP NOT NULL,
# MAGIC     last_seen_timestamp TIMESTAMP NOT NULL,
# MAGIC     processing_status STRING NOT NULL,
# MAGIC     run_id STRING NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import uuid

import builtin.gridpulse.registry as registry
import builtin.gridpulse.bronze as bronze


TEST_TABLE = "ops.source_file_registry_recovery_test"

test_payload = b"gridpulse-recovery-test"
test_hash = bronze.compute_sha256(test_payload)

test_key = "test_source|year=2026"
test_file = "recovery_test.bin"

test_bronze_path = bronze.build_bronze_path(
    bronze_base_path="Files/bronze/_tests/recovery",
    source_hash=test_hash,
    source_file=test_file,
)

first_run_id = str(uuid.uuid4())

metadata = {
    "source_name": "test_source",
    "logical_source_key": test_key,
    "source_url": "https://example.invalid/recovery-test",
    "source_file": test_file,
    "source_version": None,
    "file_size": len(test_payload),
    "source_hash": test_hash,
    "source_created_at": None,
    "run_id": first_run_id,
}


# 1. Initial attempt → PENDING
registry.register_payload_pending(
    spark=spark,
    payload_metadata=metadata,
    table_name=TEST_TABLE,
)

# 2. Simulate failed processing
registry.finalize_payload_failed(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    table_name=TEST_TABLE,
)

classification = registry.classify_payload(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    table_name=TEST_TABLE,
)

assert classification == "RECOVER"

print(f"After failure classification: {classification}")


# 3. Retry same payload
second_run_id = str(uuid.uuid4())

metadata["run_id"] = second_run_id

registry.register_payload_pending(
    spark=spark,
    payload_metadata=metadata,
    table_name=TEST_TABLE,
)

pending_row = (
    spark.table(TEST_TABLE)
    .collect()[0]
)

assert pending_row["processing_status"] == "PENDING"


# 4. Persist and verify Bronze payload
write_result = bronze.persist_bronze_payload(
    payload_bytes=test_payload,
    bronze_path=test_bronze_path,
    expected_hash=test_hash,
)

# 5. Finalize recovery successfully
registry.finalize_payload_success(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    bronze_path=test_bronze_path,
    table_name=TEST_TABLE,
)

final_row = (
    spark.table(TEST_TABLE)
    .collect()[0]
)

assert final_row["processing_status"] == "SUCCESS"
assert final_row["bronze_path"] == test_bronze_path

final_classification = registry.classify_payload(
    spark=spark,
    logical_source_key=test_key,
    source_hash=test_hash,
    table_name=TEST_TABLE,
)

assert final_classification == "UNCHANGED"

print("Recovery flow: FAILED → RECOVER → PENDING → SUCCESS")
print(f"Final classification: {final_classification}")
print("Bronze recovery validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

local_test_path = (
    f"/lakehouse/default/{test_bronze_path}"
)

if os.path.exists(local_test_path):
    os.remove(local_test_path)

print("Synthetic Bronze recovery artifact removed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS ops.source_file_registry_recovery_test;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import inspect
import importlib

import builtin.gridpulse.orchestration as orchestration
import builtin.gridpulse.sources as sources

importlib.invalidate_caches()
orchestration = importlib.reload(orchestration)
sources = importlib.reload(sources)

print("=== ORCHESTRATION PUBLIC API ===")

for name, obj in inspect.getmembers(orchestration):
    if inspect.isfunction(obj) and not name.startswith("_"):
        print(name, inspect.signature(obj))

print("\n=== SOURCES PUBLIC API ===")

for name, obj in inspect.getmembers(sources):
    if not name.startswith("_"):
        print(name, type(obj).__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from builtin.gridpulse.sources import SOURCE_CONFIGS

print("=== SOURCE CONFIG STRUCTURE ===")

for source_name, config in SOURCE_CONFIGS.items():
    print(f"\n{source_name}")
    print(config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
