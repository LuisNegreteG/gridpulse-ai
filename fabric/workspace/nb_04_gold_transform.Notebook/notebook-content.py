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

# MARKDOWN ********************

# ## Pre Audit Gold

# CELL ********************

import importlib
import builtin.gridpulse.gold as gold

# Refresh Python's module cache and reload the edited resource.
importlib.invalidate_caches()
gold = importlib.reload(gold)

print("Loaded from:", gold.__file__)
print(
    "REQUIRED_LINEAGE_COLUMNS available:",
    hasattr(gold, "REQUIRED_LINEAGE_COLUMNS")
)

assert hasattr(
    gold,
    "REQUIRED_LINEAGE_COLUMNS"
), "Updated gold.py is not visible to the current notebook session."

GOLD_CONTRACTS = gold.GOLD_CONTRACTS
COMMON_LINEAGE_COLUMNS = gold.COMMON_LINEAGE_COLUMNS
REQUIRED_LINEAGE_COLUMNS = gold.REQUIRED_LINEAGE_COLUMNS

print("Gold module reload passed.")
print("Required lineage:", REQUIRED_LINEAGE_COLUMNS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


for name in GOLD_CONTRACTS:
    print(f" - {name}")


audit_results = []

for dataset_name, contract in GOLD_CONTRACTS.items():
    table_name = contract["silver_table"]
    keys = contract["keys"]
    measures = contract["measures"]

    required_columns = (
        keys
        + measures
        + COMMON_LINEAGE_COLUMNS
    )

    df = spark.table(table_name)

    # Validate expected schema
    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    assert not missing_columns, (
        f"{table_name}: missing required columns: {missing_columns}"
    )

    # Basic row count
    row_count = df.count()

    # Validate business keys
    null_key_condition = " OR ".join(
        [f"`{key}` IS NULL" for key in keys]
    )

    null_key_count = (
        df.filter(F.expr(null_key_condition))
        .count()
    )

    duplicate_key_count = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Validate only lineage fields that are contractually required
    missing_lineage_condition = " OR ".join(
        [f"`{column}` IS NULL" for column in REQUIRED_LINEAGE_COLUMNS]
    )

    missing_lineage_count = (
        df.filter(F.expr(missing_lineage_condition))
        .count()
    )

    # Inspect nullability across all lineage fields.
    # Some lineage fields such as _source_created_at may legitimately be null.
    lineage_null_counts = (
        df.select([
            F.sum(
                F.col(column).isNull().cast("int")
            ).alias(column)
            for column in COMMON_LINEAGE_COLUMNS
        ])
        .collect()[0]
        .asDict()
    )

    print(f"\n=== {table_name} ===")
    print(f"Rows: {row_count}")
    print(f"Duplicate business keys: {duplicate_key_count}")
    print(f"Null business keys: {null_key_count}")
    print(f"Missing required lineage: {missing_lineage_count}")
    print("Lineage null counts:")

    for column in COMMON_LINEAGE_COLUMNS:
        print(
            f"  {column}: "
            f"{lineage_null_counts[column]}"
        )

    # GridPulse Gold-entry invariants
    assert null_key_count == 0, (
        f"{table_name}: "
        f"{null_key_count} rows have null business keys"
    )

    assert duplicate_key_count == 0, (
        f"{table_name}: "
        f"{duplicate_key_count} duplicate business keys"
    )

    assert missing_lineage_count == 0, (
        f"{table_name}: "
        f"{missing_lineage_count} rows have missing required lineage"
    )

    audit_results.append(
        (
            dataset_name,
            table_name,
            row_count,
            duplicate_key_count,
            null_key_count,
            missing_lineage_count,
        )
    )


audit_df = spark.createDataFrame(
    audit_results,
    [
        "dataset",
        "silver_table",
        "row_count",
        "duplicate_keys",
        "null_keys",
        "missing_required_lineage",
    ],
)

print("\n=== GOLD ENTRY AUDIT SUMMARY ===")
display(audit_df)

print("\nGold entry audit passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

SOURCE_TABLE = "silver.demand_hourly"

GOLD_COLUMNS = [
    "market_date",
    "hour_ending",
    "market_demand_mw",
    "ontario_demand_mw",
    "_source_name",
    "_source_file",
    "_source_url",
    "_source_hash",
    "_source_version",
    "_source_created_at",
    "_ingestion_timestamp",
    "_run_id",
]

df = spark.table(SOURCE_TABLE).select(*GOLD_COLUMNS)

print("=== PHYSICAL SCHEMA ===")
df.printSchema()

print("=== VALIDATION ===")
print("Rows:", df.count())

df.select(
    F.min("market_date").alias("min_date"),
    F.max("market_date").alias("max_date"),
    F.min("hour_ending").alias("min_hour"),
    F.max("hour_ending").alias("max_hour"),
).show(truncate=False)

display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create First Gold Fact

# CELL ********************

from pyspark.sql import functions as F

SOURCE_TABLE = "silver.demand_hourly"
TARGET_TABLE = "gold.fact_market_demand_hourly"

GOLD_COLUMNS = [
    "market_date",
    "hour_ending",
    "market_demand_mw",
    "ontario_demand_mw",
    "_source_name",
    "_source_file",
    "_source_url",
    "_source_hash",
    "_source_version",
    "_source_created_at",
    "_ingestion_timestamp",
    "_run_id",
]

# Build the source-aligned Gold staging dataset.
source_df = spark.table(SOURCE_TABLE).select(*GOLD_COLUMNS)

# Gold-entry invariants for this specific fact.
assert source_df.filter(
    F.col("market_date").isNull() |
    F.col("hour_ending").isNull()
).count() == 0, "Gold source contains null business keys."

assert (
    source_df.groupBy("market_date", "hour_ending")
    .count()
    .filter(F.col("count") > 1)
    .count()
    == 0
), "Gold source contains duplicate business keys."

source_df.createOrReplaceTempView("stg_gold_market_demand_hourly")


# Create the target with an explicit physical schema.
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.fact_market_demand_hourly (
    market_date DATE,
    hour_ending INT,
    market_demand_mw DECIMAL(18,3),
    ontario_demand_mw DECIMAL(18,3),
    _source_name STRING,
    _source_file STRING,
    _source_url STRING,
    _source_hash STRING,
    _source_version STRING,
    _source_created_at STRING,
    _ingestion_timestamp TIMESTAMP,
    _run_id STRING
)
USING DELTA
""")


# Idempotent upsert by the validated natural business key.
spark.sql("""
MERGE INTO gold.fact_market_demand_hourly AS target
USING stg_gold_market_demand_hourly AS source
ON  target.market_date = source.market_date
AND target.hour_ending = source.hour_ending

WHEN MATCHED THEN UPDATE SET
    target.market_demand_mw = source.market_demand_mw,
    target.ontario_demand_mw = source.ontario_demand_mw,
    target._source_name = source._source_name,
    target._source_file = source._source_file,
    target._source_url = source._source_url,
    target._source_hash = source._source_hash,
    target._source_version = source._source_version,
    target._source_created_at = source._source_created_at,
    target._ingestion_timestamp = source._ingestion_timestamp,
    target._run_id = source._run_id

WHEN NOT MATCHED THEN INSERT (
    market_date,
    hour_ending,
    market_demand_mw,
    ontario_demand_mw,
    _source_name,
    _source_file,
    _source_url,
    _source_hash,
    _source_version,
    _source_created_at,
    _ingestion_timestamp,
    _run_id
)
VALUES (
    source.market_date,
    source.hour_ending,
    source.market_demand_mw,
    source.ontario_demand_mw,
    source._source_name,
    source._source_file,
    source._source_url,
    source._source_hash,
    source._source_version,
    source._source_created_at,
    source._ingestion_timestamp,
    source._run_id
)
""")


# Post-write validation.
gold_df = spark.table(TARGET_TABLE)

source_count = source_df.count()
gold_count = gold_df.count()

duplicate_count = (
    gold_df.groupBy("market_date", "hour_ending")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("=== GOLD FACT VALIDATION ===")
print(f"Silver source rows : {source_count}")
print(f"Gold target rows   : {gold_count}")
print(f"Duplicate keys     : {duplicate_count}")

assert gold_count == source_count, (
    f"Row-count mismatch: Silver={source_count}, Gold={gold_count}"
)

assert duplicate_count == 0, (
    f"Gold contains {duplicate_count} duplicate business keys."
)

print("fact_market_demand_hourly validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

SOURCE_TABLE = "silver.demand_hourly"
TARGET_TABLE = "gold.fact_market_demand_hourly"

KEY_COLUMNS = [
    "market_date",
    "hour_ending",
]

GOLD_COLUMNS = [
    "market_date",
    "hour_ending",
    "market_demand_mw",
    "ontario_demand_mw",
    "_source_name",
    "_source_file",
    "_source_url",
    "_source_hash",
    "_source_version",
    "_source_created_at",
    "_ingestion_timestamp",
    "_run_id",
]

source_df = spark.table(SOURCE_TABLE).select(*GOLD_COLUMNS)
source_df.createOrReplaceTempView("stg_gold_market_demand_hourly")


def validate_gold_fact(label):
    source = spark.table(SOURCE_TABLE).select(*GOLD_COLUMNS)
    target = spark.table(TARGET_TABLE).select(*GOLD_COLUMNS)

    source_count = source.count()
    target_count = target.count()

    # Exact row comparison in both directions.
    missing_from_gold = source.exceptAll(target).count()
    unexpected_in_gold = target.exceptAll(source).count()

    duplicate_keys = (
        target.groupBy(*KEY_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"\n=== {label} ===")
    print(f"Silver rows          : {source_count}")
    print(f"Gold rows            : {target_count}")
    print(f"Missing from Gold    : {missing_from_gold}")
    print(f"Unexpected in Gold   : {unexpected_in_gold}")
    print(f"Duplicate Gold keys  : {duplicate_keys}")

    assert source_count == target_count
    assert missing_from_gold == 0
    assert unexpected_in_gold == 0
    assert duplicate_keys == 0


# Validate current state before idempotency test.
validate_gold_fact("PRE-IDEMPOTENCY VALIDATION")


MERGE_SQL = """
MERGE INTO gold.fact_market_demand_hourly AS target
USING stg_gold_market_demand_hourly AS source

ON  target.market_date = source.market_date
AND target.hour_ending = source.hour_ending

WHEN MATCHED AND NOT (
    target.market_demand_mw <=> source.market_demand_mw
    AND target.ontario_demand_mw <=> source.ontario_demand_mw
    AND target._source_name <=> source._source_name
    AND target._source_file <=> source._source_file
    AND target._source_url <=> source._source_url
    AND target._source_hash <=> source._source_hash
    AND target._source_version <=> source._source_version
    AND target._source_created_at <=> source._source_created_at
    AND target._ingestion_timestamp <=> source._ingestion_timestamp
    AND target._run_id <=> source._run_id
)
THEN UPDATE SET
    target.market_demand_mw = source.market_demand_mw,
    target.ontario_demand_mw = source.ontario_demand_mw,
    target._source_name = source._source_name,
    target._source_file = source._source_file,
    target._source_url = source._source_url,
    target._source_hash = source._source_hash,
    target._source_version = source._source_version,
    target._source_created_at = source._source_created_at,
    target._ingestion_timestamp = source._ingestion_timestamp,
    target._run_id = source._run_id

WHEN NOT MATCHED THEN INSERT (
    market_date,
    hour_ending,
    market_demand_mw,
    ontario_demand_mw,
    _source_name,
    _source_file,
    _source_url,
    _source_hash,
    _source_version,
    _source_created_at,
    _ingestion_timestamp,
    _run_id
)
VALUES (
    source.market_date,
    source.hour_ending,
    source.market_demand_mw,
    source.ontario_demand_mw,
    source._source_name,
    source._source_file,
    source._source_url,
    source._source_hash,
    source._source_version,
    source._source_created_at,
    source._ingestion_timestamp,
    source._run_id
)
"""


# Same input, first repeat.
spark.sql(MERGE_SQL)
validate_gold_fact("IDEMPOTENCY PASS 1")

# Same input, second repeat.
spark.sql(MERGE_SQL)
validate_gold_fact("IDEMPOTENCY PASS 2")

print("\nfact_market_demand_hourly content and idempotency validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TABLES_TO_VALIDATE = [
    "silver.demand_zonal_hourly",
    "silver.generation_hourly",
    "silver.price_day_ahead_hourly",
    "silver.price_realtime_5min",
]

for table_name in TABLES_TO_VALIDATE:
    df = spark.table(table_name)

    print("\n" + "=" * 80)
    print(table_name)
    print("=" * 80)

    df.printSchema()
    print(f"Rows: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
import builtin.gridpulse.gold as gold

importlib.invalidate_caches()
gold = importlib.reload(gold)

DATASETS = [
    "zonal_demand_hourly",
    "generation_hourly",
    "day_ahead_price_hourly",
    "realtime_price_5min",
]

results = []

print("=== INITIAL GOLD MERGE ===")

for dataset_name in DATASETS:
    print(f"\nProcessing: {dataset_name}")

    gold.merge_gold_fact(
        spark,
        dataset_name,
    )

    result = gold.validate_gold_fact(
        spark,
        dataset_name,
    )

    results.append(result)

    print(
        f"Passed: {result['gold_rows']} rows | "
        f"duplicates={result['duplicate_keys']}"
    )


print("\n=== IDEMPOTENCY PASS ===")

for dataset_name in DATASETS:
    gold.merge_gold_fact(
        spark,
        dataset_name,
    )

    gold.validate_gold_fact(
        spark,
        dataset_name,
    )

    print(f"Idempotency passed: {dataset_name}")


print("\nAll remaining Gold facts passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib
from pyspark.sql import functions as F

import builtin.gridpulse.gold as gold

importlib.invalidate_caches()
gold = importlib.reload(gold)


DATASETS = [
    "market_demand_hourly",
    "zonal_demand_hourly",
    "generation_hourly",
    "day_ahead_price_hourly",
    "realtime_price_5min",
]


# ---------------------------------------------------------
# 1. Full Silver -> Gold regression audit
# ---------------------------------------------------------

regression_results = []

print("=== GOLD REGRESSION AUDIT ===")

for dataset_name in DATASETS:
    contract = gold.GOLD_CONTRACTS[dataset_name]

    result = gold.validate_gold_fact(
        spark,
        dataset_name,
    )

    target = spark.table(contract["gold_table"])

    duplicate_keys = (
        target.groupBy(*contract["keys"])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_required_lineage = target.filter(
        F.expr(
            " OR ".join(
                [
                    f"`{column}` IS NULL"
                    for column in gold.REQUIRED_LINEAGE_COLUMNS
                ]
            )
        )
    ).count()

    assert duplicate_keys == 0
    assert missing_required_lineage == 0

    regression_results.append(
        (
            dataset_name,
            result["silver_rows"],
            result["gold_rows"],
            result["missing_from_gold"],
            result["unexpected_in_gold"],
            duplicate_keys,
            missing_required_lineage,
        )
    )


regression_df = spark.createDataFrame(
    regression_results,
    [
        "dataset",
        "silver_rows",
        "gold_rows",
        "missing_from_gold",
        "unexpected_in_gold",
        "duplicate_keys",
        "missing_required_lineage",
    ],
)

display(regression_df)


# ---------------------------------------------------------
# 2. Generation NULL preservation
# ---------------------------------------------------------

silver_generation = spark.table(
    "silver.generation_hourly"
)

gold_generation = spark.table(
    "gold.fact_generation_hourly"
)

silver_generation_nulls = (
    silver_generation
    .filter(F.col("output_mwh").isNull())
    .count()
)

gold_generation_nulls = (
    gold_generation
    .filter(F.col("output_mwh").isNull())
    .count()
)

assert silver_generation_nulls == gold_generation_nulls

print("\n=== GENERATION NULL PRESERVATION ===")
print(f"Silver NULL output_mwh : {silver_generation_nulls}")
print(f"Gold NULL output_mwh   : {gold_generation_nulls}")


# ---------------------------------------------------------
# 3. Price NULL / negative-value preservation
# ---------------------------------------------------------

PRICE_DATASETS = [
    (
        "Day-Ahead",
        "silver.price_day_ahead_hourly",
        "gold.fact_day_ahead_price_hourly",
        [
            "zonal_price_cad_per_mwh",
            "loss_price_capped_cad_per_mwh",
            "congestion_price_capped_cad_per_mwh",
        ],
    ),
    (
        "Real-Time",
        "silver.price_realtime_5min",
        "gold.fact_realtime_price_5min",
        [
            "zonal_price_capped_cad_per_mwh",
            "loss_price_capped_cad_per_mwh",
            "congestion_price_capped_cad_per_mwh",
        ],
    ),
]


for label, silver_table, gold_table, columns in PRICE_DATASETS:
    silver_df = spark.table(silver_table)
    gold_df = spark.table(gold_table)

    print(f"\n=== {label.upper()} PRICE PRESERVATION ===")

    for column in columns:
        silver_nulls = silver_df.filter(
            F.col(column).isNull()
        ).count()

        gold_nulls = gold_df.filter(
            F.col(column).isNull()
        ).count()

        silver_negatives = silver_df.filter(
            F.col(column) < 0
        ).count()

        gold_negatives = gold_df.filter(
            F.col(column) < 0
        ).count()

        assert silver_nulls == gold_nulls
        assert silver_negatives == gold_negatives

        print(
            f"{column}: "
            f"NULL silver/gold={silver_nulls}/{gold_nulls} | "
            f"negative silver/gold={silver_negatives}/{gold_negatives}"
        )


print("\nGold regression audit passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


# ---------------------------------------------------------
# Load Gold facts
# ---------------------------------------------------------

demand = spark.table("gold.fact_market_demand_hourly")
zonal = spark.table("gold.fact_zonal_demand_hourly")
generation = spark.table("gold.fact_generation_hourly")
da = spark.table("gold.fact_day_ahead_price_hourly")
rt = spark.table("gold.fact_realtime_price_5min")


# ---------------------------------------------------------
# 1. Coverage summary
# ---------------------------------------------------------

coverage_rows = []

coverage_specs = [
    ("market_demand_hourly", demand, "market_date"),
    ("zonal_demand_hourly", zonal, "market_date"),
    ("generation_hourly", generation, "market_date"),
    ("day_ahead_price_hourly", da, "market_date"),
    ("realtime_price_5min", rt, "delivery_date"),
]

for name, df, date_col in coverage_specs:
    row = (
        df.agg(
            F.count("*").alias("rows"),
            F.min(date_col).alias("min_date"),
            F.max(date_col).alias("max_date"),
        )
        .first()
    )

    coverage_rows.append(
        (
            name,
            row["rows"],
            row["min_date"],
            row["max_date"],
        )
    )

coverage_df = spark.createDataFrame(
    coverage_rows,
    ["dataset", "rows", "min_date", "max_date"],
)

print("=== GOLD COVERAGE ===")
display(coverage_df)


# ---------------------------------------------------------
# 2. Build distinct hourly-key views
# ---------------------------------------------------------

demand_hours = demand.select(
    "market_date",
    "hour_ending",
).distinct()

generation_hours = generation.select(
    "market_date",
    "hour_ending",
).distinct()

zonal_hours = zonal.select(
    "market_date",
    "hour_ending",
).distinct()

da_hours = da.select(
    "market_date",
    "hour_ending",
).distinct()

# RT remains 5-minute grain.
# This projection is ONLY for coverage analysis.
# It does not define an RT -> hourly price aggregation.
rt_hour_presence = (
    rt.select(
        F.col("delivery_date").alias("market_date"),
        F.col("delivery_hour").alias("hour_ending"),
    )
    .distinct()
)


# ---------------------------------------------------------
# 3. Trusted intersections
# ---------------------------------------------------------

def intersection_count(left, right):
    return (
        left.join(
            right,
            ["market_date", "hour_ending"],
            "inner",
        )
        .count()
    )


intersection_results = [
    (
        "Demand x Generation",
        demand_hours.count(),
        generation_hours.count(),
        intersection_count(demand_hours, generation_hours),
    ),
    (
        "Demand x Zonal Demand",
        demand_hours.count(),
        zonal_hours.count(),
        intersection_count(demand_hours, zonal_hours),
    ),
    (
        "Demand x Day-Ahead",
        demand_hours.count(),
        da_hours.count(),
        intersection_count(demand_hours, da_hours),
    ),
    (
        "Day-Ahead x RT hour presence",
        da_hours.count(),
        rt_hour_presence.count(),
        intersection_count(da_hours, rt_hour_presence),
    ),
]

intersection_df = spark.createDataFrame(
    intersection_results,
    [
        "comparison",
        "left_hour_keys",
        "right_hour_keys",
        "trusted_intersection_hour_keys",
    ],
)

print("\n=== TRUSTED COVERAGE INTERSECTIONS ===")
display(intersection_df)


# ---------------------------------------------------------
# 4. Grain-multiplication evidence
# ---------------------------------------------------------

generation_multiplicity = (
    generation.groupBy(
        "market_date",
        "hour_ending",
    )
    .count()
)

zonal_multiplicity = (
    zonal.groupBy(
        "market_date",
        "hour_ending",
    )
    .count()
)

rt_multiplicity = (
    rt.groupBy(
        "delivery_date",
        "delivery_hour",
    )
    .count()
)


generation_max = generation_multiplicity.agg(
    F.max("count")
).first()[0]

zonal_max = zonal_multiplicity.agg(
    F.max("count")
).first()[0]

rt_max = rt_multiplicity.agg(
    F.max("count")
).first()[0]


print("\n=== JOIN GRAIN SAFETY ===")
print(
    "Demand -> Generation max rows per hourly key:",
    generation_max,
)
print(
    "Demand -> Zonal Demand max rows per hourly key:",
    zonal_max,
)
print(
    "Hourly -> RT max intervals per delivery hour:",
    rt_max,
)


assert generation_max >= 1
assert zonal_max >= 1
assert rt_max >= 1


# ---------------------------------------------------------
# 5. Validate known 1:1 hourly facts
# ---------------------------------------------------------

demand_duplicate_hours = (
    demand.groupBy("market_date", "hour_ending")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

da_duplicate_hours = (
    da.groupBy("market_date", "hour_ending")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert demand_duplicate_hours == 0
assert da_duplicate_hours == 0


print("\nTrusted coverage and join-safety audit passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

bq_readiness = [
    Row(
        business_question="BQ01",
        status="READY",
        required_data="Demand",
        reason="Hourly demand coverage is available."
    ),
    Row(
        business_question="BQ02",
        status="READY",
        required_data="Demand",
        reason="Hourly demand coverage is available."
    ),
    Row(
        business_question="BQ03",
        status="READY",
        required_data="Zonal Demand",
        reason="Validated zonal hourly coverage is available."
    ),
    Row(
        business_question="BQ04",
        status="READY_WITH_INTERSECTION",
        required_data="Demand + Generation",
        reason="5712 trusted hourly keys currently overlap."
    ),
    Row(
        business_question="BQ05",
        status="READY_LIMITED_COVERAGE",
        required_data="Day-Ahead Price",
        reason="Only the explicitly ingested 24-hour Day-Ahead report is available."
    ),
    Row(
        business_question="BQ06",
        status="NOT_READY",
        required_data="Day-Ahead + Real-Time",
        reason="Current Day-Ahead and Real-Time datasets have zero overlapping hourly keys."
    ),
    Row(
        business_question="BQ07",
        status="NOT_READY",
        required_data="Day-Ahead + Real-Time",
        reason="Current Day-Ahead and Real-Time datasets have zero overlapping hourly keys."
    ),
    Row(
        business_question="BQ08",
        status="PARTIALLY_READY",
        required_data="Demand + Generation + Price",
        reason="Demand and Generation overlap, but price coverage is currently source-specific and limited."
    ),
    Row(
        business_question="BQ09",
        status="NOT_DEFINED",
        required_data="Multiple Gold facts",
        reason="No approved anomaly definition exists yet."
    ),
    Row(
        business_question="BQ10",
        status="READY",
        required_data="Gold + ops DQ/lineage",
        reason="DQ, lineage, coverage, and Gold regression evidence are available."
    ),
]

bq_readiness_df = spark.createDataFrame(bq_readiness)

display(bq_readiness_df)

print("BQ readiness audit completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
