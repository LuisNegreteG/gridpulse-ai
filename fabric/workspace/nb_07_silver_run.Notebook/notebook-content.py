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

import importlib

from pyspark.sql import functions as F

import builtin.gridpulse.silver as silver
import builtin.gridpulse.dq as dq

import builtin.gridpulse.parsers.demand as demand_parser
import builtin.gridpulse.parsers.demand_zonal as zonal_parser
import builtin.gridpulse.parsers.generation as generation_parser
import builtin.gridpulse.parsers.price_day_ahead as da_parser
import builtin.gridpulse.parsers.price_realtime as rt_parser

importlib.invalidate_caches()

silver = importlib.reload(silver)
dq = importlib.reload(dq)

demand_parser = importlib.reload(demand_parser)
zonal_parser = importlib.reload(zonal_parser)
generation_parser = importlib.reload(generation_parser)
da_parser = importlib.reload(da_parser)
rt_parser = importlib.reload(rt_parser)

spark.conf.set("spark.sql.session.timeZone", "UTC")

assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

REQUIRED_TABLES = [
    "silver.demand_hourly",
    "silver.demand_zonal_hourly",
    "silver.generation_hourly",
    "silver.price_day_ahead_hourly",
    "silver.price_realtime_5min",
    "ops.dq_result",
]

for table_name in REQUIRED_TABLES:
    assert spark.catalog.tableExists(table_name), (
        f"Required table does not exist: {table_name}"
    )

print("GridPulse Silver production runner initialized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_single_current_payload(source_name):
    rows = (
        silver.get_current_successful_payloads(
            spark=spark,
            source_name=source_name,
        )
        .collect()
    )

    assert len(rows) == 1, (
        f"{source_name}: expected exactly one current logical payload, "
        f"found {len(rows)}."
    )

    assert rows[0]["processing_status"] == "SUCCESS"
    assert rows[0]["bronze_path"] is not None

    return rows[0]


def assert_fail_free(source_name, failures):
    failed_total = sum(failures.values())

    assert failed_total == 0, (
        f"{source_name}: FAIL-level DQ detected: {failures}"
    )


def validate_merge(source_df, target_table, keys):
    target_df = spark.table(target_table)

    duplicate_keys = (
        target_df
        .groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_incoming_keys = (
        source_df
        .select(*keys)
        .join(
            target_df.select(*keys),
            on=keys,
            how="left_anti",
        )
        .count()
    )

    assert duplicate_keys == 0, (
        f"{target_table}: duplicate business keys detected"
    )

    assert missing_incoming_keys == 0, (
        f"{target_table}: incoming keys missing after MERGE"
    )


print("=== SRC-001 DEMAND ===")

demand_registry_row = get_single_current_payload(
    "ieso_hourly_demand"
)

demand_raw_df = demand_parser.parse_hourly_demand(
    spark=spark,
    bronze_path=demand_registry_row["bronze_path"],
)

demand_typed_df = demand_parser.type_hourly_demand(
    demand_raw_df
)

demand_dq_results = dq.validate_hourly_demand(
    demand_typed_df
)

assert_fail_free(
    "ieso_hourly_demand",
    demand_dq_results,
)

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

silver.merge_into_silver(
    spark=spark,
    source_df=demand_silver_df,
    target_table="silver.demand_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
    ],
)

validate_merge(
    demand_silver_df,
    "silver.demand_hourly",
    ["market_date", "hour_ending"],
)

print(f"Demand rows processed: {demand_silver_df.count():,}")


print("\n=== SRC-002 ZONAL DEMAND ===")

zonal_registry_row = get_single_current_payload(
    "ieso_hourly_zonal_demand"
)

zonal_raw_df = zonal_parser.parse_hourly_zonal_demand(
    spark=spark,
    bronze_path=zonal_registry_row["bronze_path"],
)

zonal_typed_df = zonal_parser.type_hourly_zonal_demand(
    zonal_raw_df
)

zonal_unpivoted_df = zonal_parser.unpivot_zonal_demand(
    zonal_typed_df
)

zonal_dq_results = dq.validate_hourly_zonal_demand(
    typed_df=zonal_typed_df,
    unpivoted_df=zonal_unpivoted_df,
)

assert_fail_free(
    "ieso_hourly_zonal_demand",
    zonal_dq_results["fail"],
)

zonal_silver_df = silver.add_lineage_columns(
    zonal_unpivoted_df,
    zonal_registry_row,
)

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

validate_merge(
    zonal_silver_df,
    "silver.demand_zonal_hourly",
    ["market_date", "hour_ending", "zone"],
)

print(f"Zonal rows processed: {zonal_silver_df.count():,}")


# Cross-source reconciliation.
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
    demand_current_df
    .join(
        zonal_aggregate_df,
        ["market_date", "hour_ending"],
        "inner",
    )
    .withColumn(
        "difference_mw",
        F.col("src001_ontario_demand_mw")
        - F.col("src002_ontario_demand_mw"),
    )
)

overlap_count = reconciliation_df.count()

mismatch_count = (
    reconciliation_df
    .filter(F.col("difference_mw") != 0)
    .count()
)

assert overlap_count > 0

print(
    f"Demand reconciliation: "
    f"{overlap_count:,} checked / "
    f"{mismatch_count:,} WARN mismatch(es)"
)


print("\n=== SRC-003 GENERATION ===")

generation_registry_row = get_single_current_payload(
    "ieso_generation_by_fuel_hourly"
)

generation_raw_df = generation_parser.parse_generation_hourly(
    spark=spark,
    bronze_path=generation_registry_row["bronze_path"],
)

generation_typed_df = generation_parser.type_generation_hourly(
    generation_raw_df
)

generation_dq_results = dq.validate_generation_hourly(
    generation_typed_df
)

assert_fail_free(
    "ieso_generation_by_fuel_hourly",
    generation_dq_results["fail"],
)

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

validate_merge(
    generation_silver_df,
    "silver.generation_hourly",
    ["market_date", "hour_ending", "fuel_type"],
)

print(f"Generation rows processed: {generation_silver_df.count():,}")


print("\n=== SRC-004 DAY-AHEAD PRICE ===")

da_registry_row = get_single_current_payload(
    "ieso_day_ahead_ontario_zonal_price"
)

da_raw_df = da_parser.parse_day_ahead_price(
    spark=spark,
    bronze_path=da_registry_row["bronze_path"],
)

da_typed_df = da_parser.type_day_ahead_price(
    da_raw_df
)

da_dq_results = dq.validate_day_ahead_price(
    da_typed_df
)

assert_fail_free(
    "ieso_day_ahead_ontario_zonal_price",
    da_dq_results["fail"],
)

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

silver.merge_into_silver(
    spark=spark,
    source_df=da_silver_df,
    target_table="silver.price_day_ahead_hourly",
    key_columns=[
        "market_date",
        "hour_ending",
    ],
)

validate_merge(
    da_silver_df,
    "silver.price_day_ahead_hourly",
    ["market_date", "hour_ending"],
)

print(f"Day-Ahead rows processed: {da_silver_df.count():,}")


print("\n=== SRC-005 REAL-TIME PRICE ===")

rt_registry_row = get_single_current_payload(
    "ieso_realtime_ontario_zonal_price"
)

rt_raw_df = rt_parser.parse_realtime_price(
    spark=spark,
    bronze_path=rt_registry_row["bronze_path"],
)

rt_typed_df = rt_parser.type_realtime_price(
    rt_raw_df
)

rt_dq_results = dq.validate_realtime_price(
    rt_typed_df
)

assert_fail_free(
    "ieso_realtime_ontario_zonal_price",
    rt_dq_results["fail"],
)

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

validate_merge(
    rt_silver_df,
    "silver.price_realtime_5min",
    ["delivery_date", "delivery_hour", "interval"],
)

print(f"Real-Time rows processed: {rt_silver_df.count():,}")

print("\nAll Silver transformations passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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


# SRC-001
demand_count = demand_typed_df.count()

for rule_id, failed_count in demand_dq_results.items():
    add_rule(
        registry_row=demand_registry_row,
        dataset_name="silver.demand_hourly",
        rule_id=rule_id,
        rule_category="STRUCTURE_OR_GRAIN",
        failed_count=failed_count,
        records_checked=demand_count,
    )


# SRC-002
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


# SRC-003
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


# SRC-004
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


# SRC-005
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
        details="Observed Real-Time interval population state.",
    )


dq.upsert_dq_results(
    spark=spark,
    rows=dq_rows,
)

print(f"DQ results persisted/upserted: {len(dq_rows)}")

fail_count = sum(
    1 for row in dq_rows
    if row["severity"] == "FAIL"
)

error_count = sum(
    1 for row in dq_rows
    if row["status"] == "ERROR"
)

assert fail_count == 0
assert error_count == 0

print(f"DQ FAIL: {fail_count}")
print(f"DQ ERROR: {error_count}")

print("\nGRIDPULSE SILVER RUN PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_TABLES = [
    "silver.demand_hourly",
    "silver.demand_zonal_hourly",
    "silver.generation_hourly",
    "silver.price_day_ahead_hourly",
    "silver.price_realtime_5min",
]

print("=== SILVER STEADY-STATE AUDIT ===")

for table_name in SILVER_TABLES:

    history = spark.sql(
        f"DESCRIBE HISTORY {table_name}"
    )

    latest = (
        history
        .select(
            "version",
            "operation",
            "operationMetrics",
        )
        .orderBy("version", ascending=False)
        .first()
    )

    latest_version = int(latest["version"])
    metrics = latest["operationMetrics"] or {}

    # Read only Change Data Feed emitted by the latest Delta commit.
    cdf_latest = (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", latest_version)
        .option("endingVersion", latest_version)
        .table(table_name)
    )

    cdf_rows = cdf_latest.count()

    print(f"\n{table_name}")
    print(f"Latest version       : {latest_version}")
    print(f"Latest operation     : {latest['operation']}")
    print(
        "Target rows updated :",
        metrics.get("numTargetRowsUpdated", "N/A")
    )
    print(
        "Target rows inserted:",
        metrics.get("numTargetRowsInserted", "N/A")
    )
    print(
        "Target rows deleted :",
        metrics.get("numTargetRowsDeleted", "N/A")
    )
    print(f"CDF rows latest commit: {cdf_rows}")

print("\nSilver steady-state audit completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_TABLES = [
    "silver.demand_hourly",
    "silver.demand_zonal_hourly",
    "silver.generation_hourly",
    "silver.price_day_ahead_hourly",
    "silver.price_realtime_5min",
]

versions_before = {}

print("=== BEFORE NO-OP RUN ===")

for table_name in SILVER_TABLES:
    version = int(
        spark.sql(
            f"DESCRIBE HISTORY {table_name} LIMIT 1"
        )
        .select("version")
        .first()[0]
    )

    versions_before[table_name] = version
    print(f"{table_name}: {version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== AFTER NO-OP RUN ===")

all_unchanged = True

for table_name in SILVER_TABLES:
    version_after = int(
        spark.sql(
            f"DESCRIBE HISTORY {table_name} LIMIT 1"
        )
        .select("version")
        .first()[0]
    )

    version_before = versions_before[table_name]

    changed = version_after != version_before

    print(
        f"{table_name}: "
        f"{version_before} -> {version_after} | "
        f"new_commit={changed}"
    )

    if changed:
        all_unchanged = False

assert all_unchanged, (
    "At least one Silver table created a commit during an unchanged run."
)

print("\nSILVER STEADY-STATE NO-OP PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
