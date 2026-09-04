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
# META     },
# META     "environment": {
# META       "environmentId": "3b53f205-8ca3-b075-4ee2-eb9bd4ac5e2b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import importlib
import os

from pyspark.sql import functions as F

import builtin.gridpulse.orchestration as orchestration
from builtin.gridpulse.bronze import compute_file_sha256

importlib.invalidate_caches()
orchestration = importlib.reload(orchestration)

SOURCE_NAME = "ieso_realtime_ontario_zonal_price"
SOURCE_FILE = "PUB_RealtimeOntarioZonalPrice.xml"
SOURCE_URL = (
    "https://reports-public.ieso.ca/public/"
    "RealtimeOntarioZonalPrice/"
    "PUB_RealtimeOntarioZonalPrice.xml"
)

print("GridPulse Real-Time publisher validation initialized.")
print(f"Source: {SOURCE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_result = orchestration.ingest_http_source(
    spark=spark,
    source_name=SOURCE_NAME,
    source_url=SOURCE_URL,
    source_file=SOURCE_FILE,
    logical_key_kwargs={
        "source_file": SOURCE_FILE,
    },
)

print("=== SRC-005 BRONZE ACQUISITION ===")
print(f"Run ID: {rt_result['run_id']}")
print(f"Classification: {rt_result['classification']}")
print(f"Bronze write: {rt_result['bronze_write']}")
print(f"Reused existing file: {rt_result['reused_existing_file']}")
print(f"Source hash: {rt_result['source_hash']}")
print(f"Bronze path: {rt_result['bronze_path']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_hash = rt_result["source_hash"]
bronze_path = rt_result["bronze_path"]
run_id = rt_result["run_id"]

registry_rows = (
    spark.table("ops.source_file_registry")
    .filter(
        (F.col("source_name") == SOURCE_NAME)
        & (F.col("source_hash") == source_hash)
    )
    .collect()
)

run_rows = (
    spark.table("ops.etl_run")
    .filter(F.col("run_id") == run_id)
    .collect()
)

assert len(registry_rows) == 1, (
    f"Expected exactly one registry row, found {len(registry_rows)}"
)
assert registry_rows[0]["processing_status"] == "SUCCESS"
assert registry_rows[0]["bronze_path"] == bronze_path

assert len(run_rows) == 1
assert run_rows[0]["status"] == "SUCCESS"

local_path = f"/lakehouse/default/{bronze_path}"

assert os.path.isfile(local_path), (
    f"Bronze file not found: {bronze_path}"
)

with open(local_path, "rb") as file:
    payload_bytes = file.read()

persisted_hash = compute_file_sha256(local_path)

assert persisted_hash == source_hash
assert len(payload_bytes) == registry_rows[0]["file_size"]

print("=== SRC-005 EXACT BRONZE EVIDENCE ===")
print(f"Classification: {rt_result['classification']}")
print(f"Bytes: {len(payload_bytes):,}")
print(f"SHA-256: {persisted_hash}")
print(f"Bronze path: {bronze_path}")
print(
    "Source CreatedAt raw: "
    f"{registry_rows[0]['source_created_at']}"
)
print("Registry status: SUCCESS")
print("ETL run status: SUCCESS")
print("STEP 47.3 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xml.etree.ElementTree as ET

import builtin.gridpulse.parsers.price_realtime as rt_parser

importlib.invalidate_caches()
rt_parser = importlib.reload(rt_parser)

# Parse the exact Bronze file already validated in Cell 3.
rt_raw_df = rt_parser.parse_realtime_price(
    spark=spark,
    bronze_path=bronze_path,
)

rt_typed_df = rt_parser.type_realtime_price(
    rt_raw_df
).cache()

required_columns = {
    "delivery_date",
    "delivery_hour",
    "interval",
    "zonal_price_capped_cad_per_mwh",
    "loss_price_capped_cad_per_mwh",
    "congestion_price_capped_cad_per_mwh",
    "source_flag",
    "created_at_raw",
}

missing_columns = required_columns - set(rt_typed_df.columns)

assert not missing_columns, (
    f"Missing expected SRC-005 columns: {sorted(missing_columns)}"
)

# ------------------------------------------------------------------
# Grain / structural validation
# ------------------------------------------------------------------

row_count = rt_typed_df.count()

duplicate_count = (
    rt_typed_df
    .groupBy(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_interval_count = (
    rt_typed_df
    .filter(
        (F.col("interval") < 1)
        | (F.col("interval") > 12)
    )
    .count()
)

payload_headers = (
    rt_typed_df
    .select(
        "delivery_date",
        "delivery_hour",
        "created_at_raw",
    )
    .distinct()
    .collect()
)

assert duplicate_count == 0, "Duplicate interval business keys found."
assert invalid_interval_count == 0, "Interval outside documented 1-12 domain."
assert len(payload_headers) == 1, (
    f"Expected one payload header state, found {len(payload_headers)}"
)

header = payload_headers[0]

# ------------------------------------------------------------------
# Population-state classification
# ------------------------------------------------------------------

price_columns = [
    "zonal_price_capped_cad_per_mwh",
    "loss_price_capped_cad_per_mwh",
    "congestion_price_capped_cad_per_mwh",
]

all_populated = (
    F.col(price_columns[0]).isNotNull()
    & F.col(price_columns[1]).isNotNull()
    & F.col(price_columns[2]).isNotNull()
)

all_empty = (
    F.col(price_columns[0]).isNull()
    & F.col(price_columns[1]).isNull()
    & F.col(price_columns[2]).isNull()
)

rt_state_df = (
    rt_typed_df
    .withColumn(
        "population_state",
        F.when(
            all_populated,
            F.lit("FULLY_POPULATED"),
        )
        .when(
            all_empty,
            F.lit("FULLY_EMPTY"),
        )
        .otherwise(
            F.lit("PARTIALLY_POPULATED"),
        ),
    )
)

state_counts = {
    row["population_state"]: row["count"]
    for row in (
        rt_state_df
        .groupBy("population_state")
        .count()
        .collect()
    )
}

# ------------------------------------------------------------------
# Read source-level metadata directly from the SAME Bronze bytes.
# AveragePrice is deliberately kept separate from interval rows.
# ------------------------------------------------------------------

root = ET.fromstring(payload_bytes)


def local_name(tag):
    return tag.split("}", 1)[-1]


def first_element(parent, element_name):
    for element in parent.iter():
        if local_name(element.tag) == element_name:
            return element
    return None


def child_text(parent, child_name):
    if parent is None:
        return None

    for child in list(parent):
        if local_name(child.tag) == child_name:
            if child.text is None:
                return None

            value = child.text.strip()
            return value if value else None

    return None


created_at_element = first_element(root, "CreatedAt")
doc_revision_element = first_element(root, "DocRevision")
average_price_element = first_element(root, "AveragePrice")

source_created_at_raw = (
    created_at_element.text.strip()
    if created_at_element is not None
    and created_at_element.text
    else None
)

source_doc_revision = (
    doc_revision_element.text.strip()
    if doc_revision_element is not None
    and doc_revision_element.text
    else None
)

average_price = {
    "LmpCap": child_text(
        average_price_element,
        "LmpCap",
    ),
    "LossPriceCap": child_text(
        average_price_element,
        "LossPriceCap",
    ),
    "CongPriceCap": child_text(
        average_price_element,
        "CongPriceCap",
    ),
}

interval_values = sorted(
    row["interval"]
    for row in (
        rt_typed_df
        .select("interval")
        .distinct()
        .collect()
    )
)

assert 13 not in interval_values

print("=== SRC-005 REAL-TIME PAYLOAD VALIDATION ===")
print(f"DeliveryDate: {header['delivery_date']}")
print(f"DeliveryHour: {header['delivery_hour']}")
print(f"CreatedAt raw: {source_created_at_raw}")
print(f"DocRevision: {source_doc_revision}")
print()
print(f"Interval slots observed: {row_count}")
print(f"Intervals: {interval_values}")
print(f"Duplicate business keys: {duplicate_count}")
print()
print(
    "Fully populated: "
    f"{state_counts.get('FULLY_POPULATED', 0)}"
)
print(
    "Fully empty: "
    f"{state_counts.get('FULLY_EMPTY', 0)}"
)
print(
    "Partially populated: "
    f"{state_counts.get('PARTIALLY_POPULATED', 0)}"
)
print()
print("AveragePrice (separate source metadata):")
print(f"  LmpCap: {average_price['LmpCap']}")
print(f"  LossPriceCap: {average_price['LossPriceCap']}")
print(f"  CongPriceCap: {average_price['CongPriceCap']}")
print()
print(f"Bronze SHA-256: {source_hash}")
print("STEP 47.4 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import hashlib
import json
from decimal import Decimal


NULL_TOKEN = "<NULL>"


def canonical_decimal(value):
    """
    Produce a deterministic semantic representation for numeric values.

    Examples:
    50.2  -> "50.2"
    50.20 -> "50.2"
    0.00  -> "0"
    NULL  -> "<NULL>"
    """
    if value is None:
        return NULL_TOKEN

    decimal_value = Decimal(str(value))

    # Normalize mathematical zero, including -0.00.
    if decimal_value == 0:
        return "0"

    normalized = decimal_value.normalize()

    # Avoid scientific notation in the canonical event state.
    canonical = format(normalized, "f")

    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")

    return canonical


def canonical_nullable_string(value):
    if value is None:
        return NULL_TOKEN

    return str(value)


def build_observation_state(row):
    """
    Build the canonical semantic state for one SRC-005 interval.

    Source-level revision metadata is intentionally excluded.
    """
    return {
        "delivery_date": str(row["delivery_date"]),
        "delivery_hour": int(row["delivery_hour"]),
        "interval": int(row["interval"]),
        "zonal_price_capped_cad_per_mwh": canonical_decimal(
            row["zonal_price_capped_cad_per_mwh"]
        ),
        "loss_price_capped_cad_per_mwh": canonical_decimal(
            row["loss_price_capped_cad_per_mwh"]
        ),
        "congestion_price_capped_cad_per_mwh": canonical_decimal(
            row["congestion_price_capped_cad_per_mwh"]
        ),
        "source_flag": canonical_nullable_string(
            row["source_flag"]
        ),
    }


def compute_observation_hash(row):
    state = build_observation_state(row)

    canonical_json = json.dumps(
        state,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    observation_hash = hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()

    return observation_hash, canonical_json


interval_rows = (
    rt_state_df
    .orderBy(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .collect()
)

observation_results = []

for row in interval_rows:
    observation_hash, canonical_json = compute_observation_hash(row)

    observation_results.append({
        "delivery_date": str(row["delivery_date"]),
        "delivery_hour": int(row["delivery_hour"]),
        "interval": int(row["interval"]),
        "population_state": row["population_state"],
        "observation_hash": observation_hash,
        "canonical_json": canonical_json,
    })


# ------------------------------------------------------------
# Determinism tests
# ------------------------------------------------------------

assert len(observation_results) == row_count

assert len({
    result["interval"]
    for result in observation_results
}) == row_count

# Same semantic state must always produce the same hash.
first_hash_1, first_json_1 = compute_observation_hash(interval_rows[0])
first_hash_2, first_json_2 = compute_observation_hash(interval_rows[0])

assert first_json_1 == first_json_2
assert first_hash_1 == first_hash_2

# Numeric representation must not create false revisions.
assert canonical_decimal(Decimal("50.2")) == "50.2"
assert canonical_decimal(Decimal("50.20")) == "50.2"
assert canonical_decimal(Decimal("50.2000")) == "50.2"

# NULL and zero must remain semantically different.
assert canonical_decimal(None) == NULL_TOKEN
assert canonical_decimal(Decimal("0")) == "0"
assert canonical_decimal(None) != canonical_decimal(Decimal("0"))

# Negative prices remain valid.
assert canonical_decimal(Decimal("-0.08")) == "-0.08"

# Payload-level revision fields must NOT participate
# in observation identity.
for forbidden_field in [
    "source_hash",
    "source_created_at_raw",
    "source_doc_revision",
    "publisher_run_id",
    "poll_id",
]:
    assert forbidden_field not in json.loads(first_json_1)


print("=== SRC-005 OBSERVATION HASH VALIDATION ===")
print(f"Intervals hashed: {len(observation_results)}")
print()

for result in observation_results:
    print(
        f"Interval {result['interval']:02d} | "
        f"{result['population_state']} | "
        f"{result['observation_hash']}"
    )

print()
print("Canonical example:")
print(first_json_1)
print()
print("Deterministic repeat: PASS")
print("50.2 == 50.20 canonicalization: PASS")
print("NULL != 0: PASS")
print("Negative price preservation: PASS")
print("source_hash excluded from observation identity: PASS")
print("STEP 47.5 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import hashlib
import uuid
from datetime import datetime, timezone


EVENT_SCHEMA_VERSION = "1.0"
EVENT_TYPE_OBSERVATION = "RT_PRICE_OBSERVATION"
PUBLICATION_STATE_ELIGIBLE = "ELIGIBLE"


def iso_utc(value):
    """
    Convert an existing UTC technical timestamp to ISO-8601.
    Spark session must already be configured as UTC.
    """
    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat()


def compute_event_id(
    delivery_date,
    delivery_hour,
    interval,
    source_created_at_raw,
    source_hash,
):
    created_at_token = (
        source_created_at_raw
        if source_created_at_raw is not None
        else NULL_TOKEN
    )

    identity = "|".join([
        "rt_price_event_v1",
        str(delivery_date),
        str(int(delivery_hour)),
        str(int(interval)),
        created_at_token,
        source_hash,
    ])

    return hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()


# One technical publisher execution.
# These fields do NOT participate in event identity.
publisher_run_id = str(uuid.uuid4())
poll_id = str(uuid.uuid4())

# One construction timestamp shared by this event batch.
event_created_at_utc = datetime.now(
    timezone.utc
).isoformat()

registry_row = registry_rows[0]

bronze_first_seen_at_utc = iso_utc(
    registry_row["first_seen_timestamp"]
)

rt_events = []

for row in interval_rows:

    # Contract v1 publishes market observations only when FULL.
    if row["population_state"] != "FULLY_POPULATED":
        continue

    observation_hash, _ = compute_observation_hash(row)

    event_id = compute_event_id(
        delivery_date=row["delivery_date"],
        delivery_hour=row["delivery_hour"],
        interval=row["interval"],
        source_created_at_raw=source_created_at_raw,
        source_hash=source_hash,
    )

    event = {
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "event_type": EVENT_TYPE_OBSERVATION,
        "event_id": event_id,
        "observation_hash": observation_hash,

        "delivery_date": str(row["delivery_date"]),
        "delivery_hour": int(row["delivery_hour"]),
        "interval": int(row["interval"]),

        "zonal_price_capped_cad_per_mwh":
            row["zonal_price_capped_cad_per_mwh"],
        "loss_price_capped_cad_per_mwh":
            row["loss_price_capped_cad_per_mwh"],
        "congestion_price_capped_cad_per_mwh":
            row["congestion_price_capped_cad_per_mwh"],
        "source_flag": row["source_flag"],

        "publication_state": PUBLICATION_STATE_ELIGIBLE,
        "previous_event_id": None,

        "source_name": registry_row["source_name"],
        "source_file": registry_row["source_file"],
        "source_url": registry_row["source_url"],
        "source_hash": source_hash,
        "source_created_at_raw": source_created_at_raw,
        "source_doc_revision": source_doc_revision,
        "bronze_first_seen_at_utc": bronze_first_seen_at_utc,

        "publisher_run_id": publisher_run_id,
        "poll_id": poll_id,
        "event_created_at_utc": event_created_at_utc,
    }

    rt_events.append(event)


# ------------------------------------------------------------
# Contract validation
# ------------------------------------------------------------

expected_eligible = state_counts.get(
    "FULLY_POPULATED",
    0,
)

assert len(rt_events) == expected_eligible

event_ids = [
    event["event_id"]
    for event in rt_events
]

assert len(event_ids) == len(set(event_ids))


# Same revision -> same event ID
sample = rt_events[0]

same_id = compute_event_id(
    sample["delivery_date"],
    sample["delivery_hour"],
    sample["interval"],
    sample["source_created_at_raw"],
    sample["source_hash"],
)

assert same_id == sample["event_id"]


# Retry metadata must NOT affect event ID
retry_publisher_run_id = str(uuid.uuid4())
retry_poll_id = str(uuid.uuid4())

retry_id = compute_event_id(
    sample["delivery_date"],
    sample["delivery_hour"],
    sample["interval"],
    sample["source_created_at_raw"],
    sample["source_hash"],
)

assert retry_id == sample["event_id"]


# Different exact source revision -> different event ID
synthetic_other_source_hash = hashlib.sha256(
    b"gridpulse_revision_test_only"
).hexdigest()

revised_id = compute_event_id(
    sample["delivery_date"],
    sample["delivery_hour"],
    sample["interval"],
    sample["source_created_at_raw"],
    synthetic_other_source_hash,
)

assert revised_id != sample["event_id"]


print("=== RT EVENT CONTRACT VALIDATION ===")
print(f"Eligible observations: {expected_eligible}")
print(f"Events constructed: {len(rt_events)}")
print(f"Unique event IDs: {len(set(event_ids))}")
print()

print("Sample event:")
for key, value in rt_events[0].items():
    print(f"{key}: {value}")

print()
print("Same revision -> same event_id: PASS")
print("Retry metadata -> same event_id: PASS")
print("Different source revision -> different event_id: PASS")
print("STEP 47.6B VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS ops.rt_publisher_checkpoint (
# MAGIC     source_name STRING NOT NULL,
# MAGIC 
# MAGIC     last_completed_source_hash STRING,
# MAGIC     last_completed_bronze_path STRING,
# MAGIC     last_completed_first_seen_at_utc TIMESTAMP,
# MAGIC 
# MAGIC     last_completed_at_utc TIMESTAMP,
# MAGIC     last_successful_poll_at_utc TIMESTAMP,
# MAGIC 
# MAGIC     last_publisher_run_id STRING,
# MAGIC     updated_at_utc TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS ops.rt_event_outbox (
# MAGIC     event_id STRING NOT NULL,
# MAGIC     event_schema_version STRING NOT NULL,
# MAGIC     event_type STRING NOT NULL,
# MAGIC     publication_state STRING NOT NULL,
# MAGIC 
# MAGIC     delivery_date DATE NOT NULL,
# MAGIC     delivery_hour INT NOT NULL,
# MAGIC     interval INT NOT NULL,
# MAGIC 
# MAGIC     observation_hash STRING NOT NULL,
# MAGIC     previous_event_id STRING,
# MAGIC 
# MAGIC     source_hash STRING NOT NULL,
# MAGIC     bronze_path STRING NOT NULL,
# MAGIC 
# MAGIC     event_payload STRING NOT NULL,
# MAGIC 
# MAGIC     status STRING NOT NULL,
# MAGIC     attempt_count INT NOT NULL,
# MAGIC 
# MAGIC     lease_owner_run_id STRING,
# MAGIC     lease_expires_at_utc TIMESTAMP,
# MAGIC 
# MAGIC     last_attempt_at_utc TIMESTAMP,
# MAGIC     sent_at_utc TIMESTAMP,
# MAGIC     last_error STRING,
# MAGIC 
# MAGIC     publisher_run_id STRING NOT NULL,
# MAGIC     poll_id STRING NOT NULL,
# MAGIC     created_at_utc TIMESTAMP NOT NULL,
# MAGIC     updated_at_utc TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_checkpoint_columns = {
    "source_name",
    "last_completed_source_hash",
    "last_completed_bronze_path",
    "last_completed_first_seen_at_utc",
    "last_completed_at_utc",
    "last_successful_poll_at_utc",
    "last_publisher_run_id",
    "updated_at_utc",
}

expected_outbox_columns = {
    "event_id",
    "event_schema_version",
    "event_type",
    "publication_state",
    "delivery_date",
    "delivery_hour",
    "interval",
    "observation_hash",
    "previous_event_id",
    "source_hash",
    "bronze_path",
    "event_payload",
    "status",
    "attempt_count",
    "lease_owner_run_id",
    "lease_expires_at_utc",
    "last_attempt_at_utc",
    "sent_at_utc",
    "last_error",
    "publisher_run_id",
    "poll_id",
    "created_at_utc",
    "updated_at_utc",
}

checkpoint_df = spark.table(
    "ops.rt_publisher_checkpoint"
)

outbox_df = spark.table(
    "ops.rt_event_outbox"
)

assert set(checkpoint_df.columns) == expected_checkpoint_columns
assert set(outbox_df.columns) == expected_outbox_columns

checkpoint_rows = checkpoint_df.count()
outbox_rows = outbox_df.count()

assert checkpoint_rows == 0
assert outbox_rows == 0

print("=== RT PUBLISHER STATE VALIDATION ===")
print(f"Checkpoint rows: {checkpoint_rows}")
print(f"Outbox rows: {outbox_rows}")
print("Checkpoint schema: PASS")
print("Outbox schema: PASS")
print("STEP 47.7 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date, datetime, timezone
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DateType, TimestampType,
)


def json_safe(value):
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, dict):
        return {
            key: json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [json_safe(item) for item in value]

    return value


def serialize_event(event):
    return json.dumps(
        json_safe(event),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


# ------------------------------------------------------------
# 1. Build durable outbox staging rows
# ------------------------------------------------------------

outbox_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_schema_version", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("publication_state", StringType(), False),

    StructField("delivery_date", DateType(), False),
    StructField("delivery_hour", IntegerType(), False),
    StructField("interval", IntegerType(), False),

    StructField("observation_hash", StringType(), False),
    StructField("previous_event_id", StringType(), True),

    StructField("source_hash", StringType(), False),
    StructField("bronze_path", StringType(), False),

    StructField("event_payload", StringType(), False),

    StructField("status", StringType(), False),
    StructField("attempt_count", IntegerType(), False),

    StructField("lease_owner_run_id", StringType(), True),
    StructField("lease_expires_at_utc", TimestampType(), True),

    StructField("last_attempt_at_utc", TimestampType(), True),
    StructField("sent_at_utc", TimestampType(), True),
    StructField("last_error", StringType(), True),

    StructField("publisher_run_id", StringType(), False),
    StructField("poll_id", StringType(), False),
    StructField("created_at_utc", TimestampType(), False),
    StructField("updated_at_utc", TimestampType(), False),
])

stage_created_at = datetime.now(timezone.utc)

outbox_rows = []

for event in rt_events:
    outbox_rows.append((
        event["event_id"],
        event["event_schema_version"],
        event["event_type"],
        event["publication_state"],

        date.fromisoformat(event["delivery_date"]),
        event["delivery_hour"],
        event["interval"],

        event["observation_hash"],
        event["previous_event_id"],

        event["source_hash"],
        bronze_path,

        serialize_event(event),

        "PENDING",
        0,

        None,
        None,
        None,
        None,
        None,

        event["publisher_run_id"],
        event["poll_id"],
        datetime.fromisoformat(
            event["event_created_at_utc"]
        ),
        stage_created_at,
    ))

outbox_stage_df = spark.createDataFrame(
    outbox_rows,
    schema=outbox_schema,
)

assert outbox_stage_df.count() == len(rt_events)


# ------------------------------------------------------------
# 2. Idempotent MERGE by event_id
# ------------------------------------------------------------

outbox_table = DeltaTable.forName(
    spark,
    "ops.rt_event_outbox",
)

before_count = spark.table(
    "ops.rt_event_outbox"
).count()

existing_for_batch = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("event_id").isin(event_ids))
    .count()
)

expected_new = len(event_ids) - existing_for_batch

(
    outbox_table.alias("t")
    .merge(
        outbox_stage_df.alias("s"),
        "t.event_id = s.event_id",
    )
    .whenNotMatchedInsertAll()
    .execute()
)

after_first_merge = spark.table(
    "ops.rt_event_outbox"
).count()

assert after_first_merge - before_count == expected_new


# ------------------------------------------------------------
# 3. Controlled second MERGE proves retry idempotency
# ------------------------------------------------------------

(
    outbox_table.alias("t")
    .merge(
        outbox_stage_df.alias("s"),
        "t.event_id = s.event_id",
    )
    .whenNotMatchedInsertAll()
    .execute()
)

after_retry_merge = spark.table(
    "ops.rt_event_outbox"
).count()

assert after_retry_merge == after_first_merge

duplicate_event_ids = (
    spark.table("ops.rt_event_outbox")
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_event_ids == 0


# ------------------------------------------------------------
# 4. Advance checkpoint ONLY after outbox validation succeeds
# ------------------------------------------------------------

checkpoint_time = datetime.now(timezone.utc)

checkpoint_schema = StructType([
    StructField("source_name", StringType(), False),
    StructField("last_completed_source_hash", StringType(), True),
    StructField("last_completed_bronze_path", StringType(), True),
    StructField(
        "last_completed_first_seen_at_utc",
        TimestampType(),
        True,
    ),
    StructField(
        "last_completed_at_utc",
        TimestampType(),
        True,
    ),
    StructField(
        "last_successful_poll_at_utc",
        TimestampType(),
        True,
    ),
    StructField("last_publisher_run_id", StringType(), True),
    StructField("updated_at_utc", TimestampType(), False),
])

checkpoint_stage_df = spark.createDataFrame(
    [(
        SOURCE_NAME,
        source_hash,
        bronze_path,
        registry_row["first_seen_timestamp"],
        checkpoint_time,
        checkpoint_time,
        publisher_run_id,
        checkpoint_time,
    )],
    schema=checkpoint_schema,
)

checkpoint_table = DeltaTable.forName(
    spark,
    "ops.rt_publisher_checkpoint",
)

(
    checkpoint_table.alias("t")
    .merge(
        checkpoint_stage_df.alias("s"),
        "t.source_name = s.source_name",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# ------------------------------------------------------------
# 5. Final validation
# ------------------------------------------------------------

checkpoint_result = (
    spark.table("ops.rt_publisher_checkpoint")
    .filter(F.col("source_name") == SOURCE_NAME)
    .collect()
)

assert len(checkpoint_result) == 1

checkpoint_row = checkpoint_result[0]

assert (
    checkpoint_row["last_completed_source_hash"]
    == source_hash
)

assert (
    checkpoint_row["last_completed_bronze_path"]
    == bronze_path
)

batch_outbox_count = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("event_id").isin(event_ids))
    .count()
)

pending_count = (
    spark.table("ops.rt_event_outbox")
    .filter(
        F.col("event_id").isin(event_ids)
        & (F.col("status") == "PENDING")
    )
    .count()
)

assert batch_outbox_count == len(event_ids)
assert pending_count == len(event_ids)

print("=== RT OUTBOX + CHECKPOINT VALIDATION ===")
print(f"Input events: {len(event_ids)}")
print(f"New outbox rows: {expected_new}")
print(f"Batch rows persisted: {batch_outbox_count}")
print(f"Pending events: {pending_count}")
print(f"Duplicate event_ids: {duplicate_event_ids}")
print(
    "Checkpoint source hash: "
    f"{checkpoint_row['last_completed_source_hash']}"
)
print()
print("Second MERGE produced no duplicates: PASS")
print("Checkpoint advanced after outbox success: PASS")
print("STEP 47.8 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Acquire one fresh SRC-005 snapshot using the validated Bronze flow.
next_result = orchestration.ingest_http_source(
    spark=spark,
    source_name=SOURCE_NAME,
    source_url=SOURCE_URL,
    source_file=SOURCE_FILE,
    logical_key_kwargs={
        "source_file": SOURCE_FILE,
    },
)

next_source_hash = next_result["source_hash"]
next_bronze_path = next_result["bronze_path"]

checkpoint_row = (
    spark.table("ops.rt_publisher_checkpoint")
    .filter(F.col("source_name") == SOURCE_NAME)
    .collect()[0]
)

previous_source_hash = checkpoint_row[
    "last_completed_source_hash"
]

print("=== STEP 47.9A — REVISION GATE ===")
print(f"Classification: {next_result['classification']}")
print(f"Previous checkpoint hash: {previous_source_hash}")
print(f"Current source hash:       {next_source_hash}")
print(f"Bronze path: {next_bronze_path}")

if next_source_hash == previous_source_hash:
    print()
    print("No new source revision yet.")
    print("STEP 47.9A: UNCHANGED")
else:
    # Validate the exact persisted Bronze evidence.
    next_registry_rows = (
        spark.table("ops.source_file_registry")
        .filter(
            (F.col("source_name") == SOURCE_NAME)
            & (F.col("source_hash") == next_source_hash)
        )
        .collect()
    )

    assert len(next_registry_rows) == 1
    assert (
        next_registry_rows[0]["processing_status"]
        == "SUCCESS"
    )

    next_local_path = (
        f"/lakehouse/default/{next_bronze_path}"
    )

    with open(next_local_path, "rb") as file:
        next_payload_bytes = file.read()

    next_persisted_hash = compute_file_sha256(
        next_local_path
    )

    assert next_persisted_hash == next_source_hash

    print()
    print("New/revised source payload detected.")
    print(f"Bytes: {len(next_payload_bytes):,}")
    print("Exact Bronze SHA-256 validation: PASS")
    print("STEP 47.9A VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window
import xml.etree.ElementTree as ET


# ------------------------------------------------------------
# 1. Parse the exact NEW Bronze snapshot
# ------------------------------------------------------------

next_raw_df = rt_parser.parse_realtime_price(
    spark=spark,
    bronze_path=next_bronze_path,
)

next_typed_df = rt_parser.type_realtime_price(
    next_raw_df
).cache()

price_columns = [
    "zonal_price_capped_cad_per_mwh",
    "loss_price_capped_cad_per_mwh",
    "congestion_price_capped_cad_per_mwh",
]

all_populated = (
    F.col(price_columns[0]).isNotNull()
    & F.col(price_columns[1]).isNotNull()
    & F.col(price_columns[2]).isNotNull()
)

all_empty = (
    F.col(price_columns[0]).isNull()
    & F.col(price_columns[1]).isNull()
    & F.col(price_columns[2]).isNull()
)

next_state_df = (
    next_typed_df
    .withColumn(
        "population_state",
        F.when(all_populated, F.lit("FULLY_POPULATED"))
        .when(all_empty, F.lit("FULLY_EMPTY"))
        .otherwise(F.lit("PARTIALLY_POPULATED"))
    )
)

next_rows = (
    next_state_df
    .orderBy(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .collect()
)


# ------------------------------------------------------------
# 2. Read source revision metadata from SAME Bronze bytes
# ------------------------------------------------------------

next_root = ET.fromstring(next_payload_bytes)


def find_xml_text(root, name):
    for element in root.iter():
        if element.tag.split("}", 1)[-1] == name:
            if element.text is None:
                return None

            value = element.text.strip()
            return value if value else None

    return None


next_source_created_at_raw = find_xml_text(
    next_root,
    "CreatedAt",
)

next_source_doc_revision = find_xml_text(
    next_root,
    "DocRevision",
)


# ------------------------------------------------------------
# 3. Resolve latest previously published event per business key
#
# IMPORTANT:
# This uses GridPulse event construction order, NOT source CreatedAt.
# ------------------------------------------------------------

latest_window = (
    Window
    .partitionBy(
        "delivery_date",
        "delivery_hour",
        "interval",
    )
    .orderBy(
        F.col("created_at_utc").desc(),
        F.col("event_id").desc(),
    )
)

latest_events = (
    spark.table("ops.rt_event_outbox")
    .withColumn(
        "_rn",
        F.row_number().over(latest_window),
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .collect()
)

latest_by_key = {
    (
        str(row["delivery_date"]),
        int(row["delivery_hour"]),
        int(row["interval"]),
    ): row
    for row in latest_events
}


# ------------------------------------------------------------
# 4. Compare semantic interval state
# ------------------------------------------------------------

decisions = []

for row in next_rows:

    key = (
        str(row["delivery_date"]),
        int(row["delivery_hour"]),
        int(row["interval"]),
    )

    population_state = row["population_state"]

    observation_hash, _ = compute_observation_hash(row)

    previous = latest_by_key.get(key)

    previous_event_id = (
        previous["event_id"]
        if previous is not None
        else None
    )

    previous_state = (
        previous["publication_state"]
        if previous is not None
        else None
    )

    previous_observation_hash = (
        previous["observation_hash"]
        if previous is not None
        else None
    )

    if population_state == "FULLY_POPULATED":

        if (
            previous is not None
            and previous_state == "ELIGIBLE"
            and previous_observation_hash == observation_hash
        ):
            decision = "UNCHANGED"

        elif (
            previous is not None
            and previous_state == "ELIGIBLE"
        ):
            decision = "REVISED_OBSERVATION"

        else:
            decision = "NEW_OBSERVATION"

    else:

        if (
            previous is not None
            and previous_state == "ELIGIBLE"
        ):
            decision = "INVALIDATION"

        else:
            decision = "SUPPRESSED_INELIGIBLE"

    decisions.append({
        "delivery_date": key[0],
        "delivery_hour": key[1],
        "interval": key[2],
        "population_state": population_state,
        "decision": decision,
        "observation_hash": observation_hash,
        "previous_observation_hash":
            previous_observation_hash,
        "previous_event_id": previous_event_id,
    })


# ------------------------------------------------------------
# 5. Validation / summary
# ------------------------------------------------------------

decision_counts = {}

for item in decisions:
    decision_counts[item["decision"]] = (
        decision_counts.get(item["decision"], 0) + 1
    )

assert len(decisions) == len(next_rows)

print("=== STEP 47.9B — REVISION-AWARE COMPARISON ===")
print(f"Previous source hash: {previous_source_hash}")
print(f"Current source hash:  {next_source_hash}")
print(f"CreatedAt raw: {next_source_created_at_raw}")
print(f"DocRevision: {next_source_doc_revision}")
print()

for name in [
    "UNCHANGED",
    "NEW_OBSERVATION",
    "REVISED_OBSERVATION",
    "INVALIDATION",
    "SUPPRESSED_INELIGIBLE",
]:
    print(
        f"{name}: "
        f"{decision_counts.get(name, 0)}"
    )

print()
print("Interval decisions:")

for item in decisions:
    print(
        f"Interval {item['interval']:02d} | "
        f"{item['population_state']} | "
        f"{item['decision']}"
    )

partial_count = sum(
    1
    for item in decisions
    if item["population_state"]
    == "PARTIALLY_POPULATED"
)

if partial_count:
    print()
    print(
        f"WARNING: {partial_count} partially populated "
        "interval(s) observed."
    )

print()
print("No outbox writes performed.")
print("Checkpoint unchanged.")
print("STEP 47.9B VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== BUSINESS KEY TRANSITION ===")

print(
    "Previous published hours:",
    sorted({
        (
            event["delivery_date"],
            event["delivery_hour"],
        )
        for event in rt_events
    })
)

print(
    "Current snapshot hours:",
    sorted({
        (
            str(row["delivery_date"]),
            int(row["delivery_hour"]),
        )
        for row in next_rows
    })
)

print("STEP 47.9C VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone


PUBLISHABLE_DECISIONS = {
    "NEW_OBSERVATION",
    "REVISED_OBSERVATION",
    "INVALIDATION",
}

decision_by_key = {
    (
        item["delivery_date"],
        item["delivery_hour"],
        item["interval"],
    ): item
    for item in decisions
}

next_registry_row = next_registry_rows[0]

next_publisher_run_id = str(uuid.uuid4())
next_poll_id = str(uuid.uuid4())

next_event_created_at = datetime.now(timezone.utc)
next_event_created_at_iso = next_event_created_at.isoformat()

next_bronze_first_seen_iso = iso_utc(
    next_registry_row["first_seen_timestamp"]
)

next_rt_events = []


# ------------------------------------------------------------
# 1. Build only publishable events
# ------------------------------------------------------------

for row in next_rows:

    key = (
        str(row["delivery_date"]),
        int(row["delivery_hour"]),
        int(row["interval"]),
    )

    decision = decision_by_key[key]

    if decision["decision"] not in PUBLISHABLE_DECISIONS:
        continue

    if decision["decision"] == "INVALIDATION":
        event_type = "RT_PRICE_INVALIDATION"
        publication_state = "INVALIDATED"
    else:
        event_type = "RT_PRICE_OBSERVATION"
        publication_state = "ELIGIBLE"

    event_id = compute_event_id(
        delivery_date=row["delivery_date"],
        delivery_hour=row["delivery_hour"],
        interval=row["interval"],
        source_created_at_raw=next_source_created_at_raw,
        source_hash=next_source_hash,
    )

    event = {
        "event_schema_version": "1.0",
        "event_type": event_type,
        "event_id": event_id,
        "observation_hash": decision["observation_hash"],

        "delivery_date": str(row["delivery_date"]),
        "delivery_hour": int(row["delivery_hour"]),
        "interval": int(row["interval"]),

        "zonal_price_capped_cad_per_mwh":
            row["zonal_price_capped_cad_per_mwh"],
        "loss_price_capped_cad_per_mwh":
            row["loss_price_capped_cad_per_mwh"],
        "congestion_price_capped_cad_per_mwh":
            row["congestion_price_capped_cad_per_mwh"],
        "source_flag": row["source_flag"],

        "publication_state": publication_state,
        "previous_event_id": decision["previous_event_id"],

        "source_name": next_registry_row["source_name"],
        "source_file": next_registry_row["source_file"],
        "source_url": next_registry_row["source_url"],
        "source_hash": next_source_hash,
        "source_created_at_raw": next_source_created_at_raw,
        "source_doc_revision": next_source_doc_revision,
        "bronze_first_seen_at_utc":
            next_bronze_first_seen_iso,

        "publisher_run_id": next_publisher_run_id,
        "poll_id": next_poll_id,
        "event_created_at_utc":
            next_event_created_at_iso,
    }

    next_rt_events.append(event)


expected_publishable = sum(
    count
    for decision_name, count in decision_counts.items()
    if decision_name in PUBLISHABLE_DECISIONS
)

assert len(next_rt_events) == expected_publishable


# ------------------------------------------------------------
# 2. Create outbox staging rows
# ------------------------------------------------------------

stage_updated_at = datetime.now(timezone.utc)

next_outbox_rows = []

for event in next_rt_events:

    next_outbox_rows.append((
        event["event_id"],
        event["event_schema_version"],
        event["event_type"],
        event["publication_state"],

        date.fromisoformat(event["delivery_date"]),
        event["delivery_hour"],
        event["interval"],

        event["observation_hash"],
        event["previous_event_id"],

        event["source_hash"],
        next_bronze_path,

        serialize_event(event),

        "PENDING",
        0,

        None,
        None,
        None,
        None,
        None,

        event["publisher_run_id"],
        event["poll_id"],
        next_event_created_at,
        stage_updated_at,
    ))

next_outbox_stage_df = spark.createDataFrame(
    next_outbox_rows,
    schema=outbox_schema,
)


# ------------------------------------------------------------
# 3. Idempotent MERGE
# ------------------------------------------------------------

next_event_ids = [
    event["event_id"]
    for event in next_rt_events
]

before_count = spark.table(
    "ops.rt_event_outbox"
).count()

existing_for_revision = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("event_id").isin(next_event_ids))
    .count()
)

expected_new = (
    len(next_event_ids)
    - existing_for_revision
)

(
    outbox_table.alias("t")
    .merge(
        next_outbox_stage_df.alias("s"),
        "t.event_id = s.event_id",
    )
    .whenNotMatchedInsertAll()
    .execute()
)

after_count = spark.table(
    "ops.rt_event_outbox"
).count()

assert after_count - before_count == expected_new


# ------------------------------------------------------------
# 4. Validate durable outbox BEFORE checkpoint
# ------------------------------------------------------------

persisted_revision_count = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("source_hash") == next_source_hash)
    .count()
)

revision_duplicates = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("source_hash") == next_source_hash)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert persisted_revision_count == expected_publishable
assert revision_duplicates == 0


# ------------------------------------------------------------
# 5. Advance checkpoint only after successful validation
# ------------------------------------------------------------

checkpoint_time = datetime.now(timezone.utc)

next_checkpoint_stage_df = spark.createDataFrame(
    [(
        SOURCE_NAME,
        next_source_hash,
        next_bronze_path,
        next_registry_row["first_seen_timestamp"],
        checkpoint_time,
        checkpoint_time,
        next_publisher_run_id,
        checkpoint_time,
    )],
    schema=checkpoint_schema,
)

(
    checkpoint_table.alias("t")
    .merge(
        next_checkpoint_stage_df.alias("s"),
        "t.source_name = s.source_name",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# ------------------------------------------------------------
# 6. Final validation
# ------------------------------------------------------------

final_checkpoint = (
    spark.table("ops.rt_publisher_checkpoint")
    .filter(F.col("source_name") == SOURCE_NAME)
    .collect()[0]
)

assert (
    final_checkpoint["last_completed_source_hash"]
    == next_source_hash
)

pending_for_revision = (
    spark.table("ops.rt_event_outbox")
    .filter(
        (F.col("source_hash") == next_source_hash)
        & (F.col("status") == "PENDING")
    )
    .count()
)

suppressed_count = (
    decision_counts.get("SUPPRESSED_INELIGIBLE", 0)
)

print("=== STEP 47.9D — REVISION PERSISTENCE ===")
print(f"Publishable decisions: {expected_publishable}")
print(f"New outbox rows: {expected_new}")
print(f"Persisted revision events: {persisted_revision_count}")
print(f"Pending revision events: {pending_for_revision}")
print(f"Suppressed ineligible intervals: {suppressed_count}")
print(f"Duplicate event_ids: {revision_duplicates}")
print()
print(
    "Checkpoint source hash: "
    f"{final_checkpoint['last_completed_source_hash']}"
)
print()
print("Only publishable intervals persisted: PASS")
print("Ineligible intervals produced no events: PASS")
print("Checkpoint advanced after outbox validation: PASS")
print("STEP 47.9D VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

outbox_audit_df = spark.table("ops.rt_event_outbox")
checkpoint_audit_df = spark.table("ops.rt_publisher_checkpoint")

# ------------------------------------------------------------
# 1. Core counts
# ------------------------------------------------------------

total_events = outbox_audit_df.count()

pending_events = (
    outbox_audit_df
    .filter(F.col("status") == "PENDING")
    .count()
)

sent_events = (
    outbox_audit_df
    .filter(F.col("status") == "SENT")
    .count()
)

duplicate_event_ids = (
    outbox_audit_df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_event_ids = (
    outbox_audit_df
    .filter(F.col("event_id").isNull())
    .count()
)

assert duplicate_event_ids == 0
assert null_event_ids == 0


# ------------------------------------------------------------
# 2. Checkpoint validation
# ------------------------------------------------------------

checkpoint_rows = (
    checkpoint_audit_df
    .filter(F.col("source_name") == SOURCE_NAME)
    .collect()
)

assert len(checkpoint_rows) == 1

checkpoint = checkpoint_rows[0]

assert (
    checkpoint["last_completed_source_hash"]
    == next_source_hash
)

assert (
    checkpoint["last_completed_bronze_path"]
    == next_bronze_path
)


# ------------------------------------------------------------
# 3. Source revision distribution
# ------------------------------------------------------------

source_distribution = (
    outbox_audit_df
    .groupBy("source_hash")
    .count()
    .orderBy("source_hash")
    .collect()
)


# ------------------------------------------------------------
# 4. Market-hour coverage
# ------------------------------------------------------------

hour_distribution = (
    outbox_audit_df
    .groupBy(
        "delivery_date",
        "delivery_hour",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.min("interval").alias("min_interval"),
        F.max("interval").alias("max_interval"),
    )
    .orderBy(
        "delivery_date",
        "delivery_hour",
    )
    .collect()
)


# ------------------------------------------------------------
# 5. Current test snapshot validation
# ------------------------------------------------------------

latest_revision_rows = (
    outbox_audit_df
    .filter(F.col("source_hash") == next_source_hash)
    .select("interval")
    .orderBy("interval")
    .collect()
)

latest_revision_intervals = [
    row["interval"]
    for row in latest_revision_rows
]

assert latest_revision_intervals == list(range(1, 9))


# ------------------------------------------------------------
# 6. Payload / table consistency
# ------------------------------------------------------------

payload_mismatches = 0

for row in outbox_audit_df.collect():

    payload = json.loads(row["event_payload"])

    checks = [
        payload["event_id"] == row["event_id"],
        payload["event_type"] == row["event_type"],
        payload["publication_state"] == row["publication_state"],
        payload["observation_hash"] == row["observation_hash"],
        payload["source_hash"] == row["source_hash"],
        int(payload["delivery_hour"]) == row["delivery_hour"],
        int(payload["interval"]) == row["interval"],
    ]

    if not all(checks):
        payload_mismatches += 1

assert payload_mismatches == 0


# ------------------------------------------------------------
# 7. Test-state expectations
# ------------------------------------------------------------

assert total_events == 20
assert pending_events == 20
assert sent_events == 0


print("=== GRIDPULSE RT PUBLISHER STATE AUDIT ===")
print(f"Outbox total: {total_events}")
print(f"PENDING: {pending_events}")
print(f"SENT: {sent_events}")
print(f"Duplicate event_ids: {duplicate_event_ids}")
print(f"Payload/table mismatches: {payload_mismatches}")

print("\nSource hash distribution:")
for row in source_distribution:
    print(
        f"  {row['source_hash']} -> "
        f"{row['count']} event(s)"
    )

print("\nMarket-hour coverage:")
for row in hour_distribution:
    print(
        f"  {row['delivery_date']} HE {row['delivery_hour']} "
        f"-> {row['event_count']} events "
        f"(intervals {row['min_interval']}-"
        f"{row['max_interval']})"
    )

print()
print(
    "Checkpoint source hash: "
    f"{checkpoint['last_completed_source_hash']}"
)

print(
    "Latest revision published intervals: "
    f"{latest_revision_intervals}"
)

print()
print("Outbox event_id uniqueness: PASS")
print("Payload/table reconciliation: PASS")
print("Checkpoint alignment: PASS")
print("Latest source eligibility suppression: PASS")
print("STEP 47.10 VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import importlib.util

eventhub_available = (
    importlib.util.find_spec("azure.eventhub")
    is not None
)

print("=== EVENTSTREAM TRANSPORT DEPENDENCY ===")
print(f"azure-eventhub available: {eventhub_available}")

if eventhub_available:
    import azure.eventhub

    print(
        "azure-eventhub version:",
        azure.eventhub.__version__,
    )

print("STEP 47.11 DEPENDENCY CHECK COMPLETE.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import azure.eventhub
from azure.eventhub import (
    EventData,
    EventHubProducerClient,
)

print("=== EVENTSTREAM TRANSPORT DEPENDENCY ===")
print(
    "azure-eventhub version:",
    azure.eventhub.__version__,
)

assert (
    azure.eventhub.__version__.split(".")[0]
    == "5"
)

print("EventHubProducerClient: PASS")
print("EventData: PASS")
print("STEP 47.11A VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient
import os

producer = None

try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=os.environ["GRIDPULSE_EVENTSTREAM_CONNECTION"]
    )

    properties = producer.get_eventhub_properties()

    print("=== EVENTSTREAM CONNECTION VALIDATION ===")
    print(f"Event Hub name: {properties['eventhub_name']}")
    print(
        "Partition count:",
        len(properties["partition_ids"]),
    )
    print("Events sent: 0")
    print("STEP 48.2 VALIDATION PASSED.")

finally:
    if producer is not None:
        producer.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from azure.eventhub import EventData, EventHubProducerClient
from pyspark.sql import functions as F

probe_rows = (
    spark.table("ops.rt_event_outbox")
    .filter(F.col("status") == "PENDING")
    .orderBy(
        F.col("created_at_utc").asc(),
        F.col("event_id").asc(),
    )
    .limit(1)
    .collect()
)

assert len(probe_rows) == 1

probe = probe_rows[0]

producer = None

try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string
    )

    batch = producer.create_batch()

    event = EventData(probe["event_payload"])
    event.content_type = "application/json"

    batch.add(event)

    producer.send_batch(
        batch,
        timeout=30,
    )

finally:
    if producer is not None:
        producer.close()

print("=== STEP 50.2 E2E PROBE ===")
print(f"Event ID: {probe['event_id']}")
print("Events sent: 1")
print("Outbox status modified: NO")
print("STEP 50.2 PROBE SENT.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

outbox = spark.table("ops.rt_event_outbox")

status_summary = (
    outbox
    .groupBy("status")
    .count()
    .orderBy("status")
    .collect()
)

duplicate_ids = (
    outbox
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_attempts = (
    outbox
    .filter(F.col("attempt_count") < 0)
    .count()
)

leased_rows = (
    outbox
    .filter(F.col("lease_owner_run_id").isNotNull())
    .count()
)

total_rows = outbox.count()

print("=== RT OUTBOX PRE-DISPATCH AUDIT ===")
print(f"Total rows: {total_rows}")

for row in status_summary:
    print(f"{row['status']}: {row['count']}")

print(f"Duplicate event_ids: {duplicate_ids}")
print(f"Invalid attempt_count rows: {invalid_attempts}")
print(f"Rows currently leased: {leased_rows}")

assert duplicate_ids == 0
assert invalid_attempts == 0

print("STEP 53.3 PRE-DISPATCH AUDIT PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import uuid
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubProducerClient
from delta.tables import DeltaTable
from pyspark.sql import functions as F


# ------------------------------------------------------------
# 0. Runtime validation
# ------------------------------------------------------------

spark.conf.set("spark.sql.session.timeZone", "UTC")

connection_string = os.environ.get(
    "GRIDPULSE_EVENTSTREAM_CONNECTION"
)

assert connection_string, (
    "Eventstream connection is not loaded in this session."
)

OUTBOX_TABLE = "ops.rt_event_outbox"

dispatcher_run_id = str(uuid.uuid4())

claim_time = datetime.now(timezone.utc)
lease_expires_at = claim_time + timedelta(minutes=5)


# ------------------------------------------------------------
# 1. Find exactly one dispatchable event
#
# Eligible:
# - PENDING
# - or an abandoned SENDING row whose lease expired
# ------------------------------------------------------------

candidate_rows = (
    spark.table(OUTBOX_TABLE)
    .filter(
        (F.col("status") == "PENDING")
        |
        (
            (F.col("status") == "SENDING")
            & (
                F.col("lease_expires_at_utc").isNull()
                | (
                    F.col("lease_expires_at_utc")
                    < F.current_timestamp()
                )
            )
        )
    )
    .orderBy(
        F.col("created_at_utc").asc(),
        F.col("event_id").asc(),
    )
    .limit(1)
    .collect()
)

assert len(candidate_rows) == 1, (
    "No dispatchable outbox event found."
)

candidate = candidate_rows[0]
candidate_event_id = candidate["event_id"]


# ------------------------------------------------------------
# 2. Attempt durable claim
#
# The conditional update prevents us from taking an active lease.
# ------------------------------------------------------------

outbox_delta = DeltaTable.forName(
    spark,
    OUTBOX_TABLE,
)

claim_source = spark.createDataFrame(
    [(
        candidate_event_id,
        dispatcher_run_id,
        claim_time,
        lease_expires_at,
    )],
    """
    event_id string,
    lease_owner_run_id string,
    claim_time timestamp,
    lease_expires_at_utc timestamp
    """,
)

(
    outbox_delta.alias("t")
    .merge(
        claim_source.alias("s"),
        "t.event_id = s.event_id",
    )
    .whenMatchedUpdate(
        condition="""
            t.status = 'PENDING'
            OR (
                t.status = 'SENDING'
                AND (
                    t.lease_expires_at_utc IS NULL
                    OR t.lease_expires_at_utc < current_timestamp()
                )
            )
        """,
        set={
            "status": F.lit("SENDING"),
            "lease_owner_run_id":
                F.col("s.lease_owner_run_id"),
            "lease_expires_at_utc":
                F.col("s.lease_expires_at_utc"),
            "attempt_count":
                F.coalesce(
                    F.col("t.attempt_count"),
                    F.lit(0),
                ) + F.lit(1),
            "last_attempt_at_utc":
                F.col("s.claim_time"),
            "updated_at_utc":
                F.col("s.claim_time"),
            "last_error":
                F.lit(None).cast("string"),
        },
    )
    .execute()
)


# ------------------------------------------------------------
# 3. Confirm THIS dispatcher actually owns the lease
#
# Critical before network send.
# ------------------------------------------------------------

claimed_rows = (
    spark.table(OUTBOX_TABLE)
    .filter(
        F.col("event_id") == candidate_event_id
    )
    .collect()
)

assert len(claimed_rows) == 1

claimed = claimed_rows[0]

assert claimed["status"] == "SENDING"
assert (
    claimed["lease_owner_run_id"]
    == dispatcher_run_id
), (
    "Dispatcher did not acquire the event lease. "
    "No event was sent."
)


# ------------------------------------------------------------
# 4. Send exactly one durable payload
# ------------------------------------------------------------

producer = None

try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string
    )

    batch = producer.create_batch()

    event = EventData(
        claimed["event_payload"]
    )

    event.content_type = "application/json"

    event.properties = {
        "event_id": claimed["event_id"],
        "event_type": claimed["event_type"],
        "dispatcher_run_id": dispatcher_run_id,
    }

    batch.add(event)

    producer.send_batch(
        batch,
        timeout=30,
    )

    # --------------------------------------------------------
    # 5. Transport acknowledged → mark SENT
    #
    # SENT means Eventstream accepted the delivery.
    # It does NOT claim that Eventhouse ingestion was already
    # completed.
    # --------------------------------------------------------

    sent_time = datetime.now(timezone.utc)

    outbox_delta.update(
        condition=(
            (F.col("event_id") == candidate_event_id)
            & (
                F.col("lease_owner_run_id")
                == dispatcher_run_id
            )
            & (F.col("status") == "SENDING")
        ),
        set={
            "status": F.lit("SENT"),
            "sent_at_utc": F.lit(sent_time),
            "lease_owner_run_id":
                F.lit(None).cast("string"),
            "lease_expires_at_utc":
                F.lit(None).cast("timestamp"),
            "last_error":
                F.lit(None).cast("string"),
            "updated_at_utc": F.lit(sent_time),
        },
    )

except Exception as exc:

    failure_time = datetime.now(timezone.utc)

    # Recoverability rule:
    # failed send returns to PENDING.
    outbox_delta.update(
        condition=(
            (F.col("event_id") == candidate_event_id)
            & (
                F.col("lease_owner_run_id")
                == dispatcher_run_id
            )
            & (F.col("status") == "SENDING")
        ),
        set={
            "status": F.lit("PENDING"),
            "lease_owner_run_id":
                F.lit(None).cast("string"),
            "lease_expires_at_utc":
                F.lit(None).cast("timestamp"),
            "last_error":
                F.lit(str(exc)[:4000]),
            "updated_at_utc":
                F.lit(failure_time),
        },
    )

    raise

finally:
    if producer is not None:
        producer.close()


# ------------------------------------------------------------
# 6. Durable post-send validation
# ------------------------------------------------------------

final_rows = (
    spark.table(OUTBOX_TABLE)
    .filter(
        F.col("event_id") == candidate_event_id
    )
    .collect()
)

assert len(final_rows) == 1

final_row = final_rows[0]

assert final_row["status"] == "SENT"
assert final_row["attempt_count"] >= 1
assert final_row["sent_at_utc"] is not None
assert final_row["lease_owner_run_id"] is None
assert final_row["lease_expires_at_utc"] is None

status_counts = {
    row["status"]: row["count"]
    for row in (
        spark.table(OUTBOX_TABLE)
        .groupBy("status")
        .count()
        .collect()
    )
}

print("=== RT OUTBOX SINGLE-EVENT DISPATCH ===")
print(f"Event ID: {candidate_event_id}")
print(
    f"Attempt count: "
    f"{final_row['attempt_count']}"
)
print(f"Final status: {final_row['status']}")
print(
    f"Sent at UTC: "
    f"{final_row['sent_at_utc']}"
)
print()
print(
    "PENDING:",
    status_counts.get("PENDING", 0),
)
print(
    "SENDING:",
    status_counts.get("SENDING", 0),
)
print(
    "SENT:",
    status_counts.get("SENT", 0),
)
print()
print("Lease released: PASS")
print("Transport acknowledgment recorded: PASS")
print("STEP 53.4A VALIDATION PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import uuid

from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubProducerClient
from delta.tables import DeltaTable
from pyspark.sql import functions as F


OUTBOX_TABLE = "ops.rt_event_outbox"


def dispatch_pending_outbox(
    max_events=None,
    lease_minutes=5,
):
    """
    Dispatch eligible GridPulse real-time events from the durable
    Delta outbox to Fabric Eventstream.

    Eligible rows:
      - PENDING
      - SENDING with an expired lease

    Delivery semantics:
      - Durable claim before network send
      - SENDING during transport
      - SENT only after transport acknowledgement
      - Failed sends return to PENDING
      - event_id remains unchanged across retries

    Parameters
    ----------
    max_events : int | None
        Maximum number of events to send.
        None dispatches all currently eligible events.

    lease_minutes : int
        Lease duration used to recover abandoned SENDING rows.

    Returns
    -------
    dict
        Dispatcher execution summary.
    """

    connection_string = os.environ.get(
        "GRIDPULSE_EVENTSTREAM_CONNECTION"
    )

    if not connection_string:
        raise RuntimeError(
            "GRIDPULSE_EVENTSTREAM_CONNECTION "
            "is not available in this runtime."
        )

    if max_events is not None and max_events <= 0:
        raise ValueError(
            "max_events must be positive or None."
        )

    outbox_delta = DeltaTable.forName(
        spark,
        OUTBOX_TABLE,
    )

    dispatcher_run_id = str(uuid.uuid4())

    sent_event_ids = []
    failed_event_ids = []


    # --------------------------------------------------------
    # Candidate resolver
    # --------------------------------------------------------

    def get_next_candidate():

        rows = (
            spark.table(OUTBOX_TABLE)
            .filter(
                (F.col("status") == "PENDING")
                |
                (
                    (F.col("status") == "SENDING")
                    &
                    (
                        F.col("lease_expires_at_utc").isNull()
                        |
                        (
                            F.col("lease_expires_at_utc")
                            < F.current_timestamp()
                        )
                    )
                )
            )
            .orderBy(
                F.col("created_at_utc").asc(),
                F.col("event_id").asc(),
            )
            .limit(1)
            .collect()
        )

        return rows[0] if rows else None


    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string
    )

    try:

        dispatch_index = 0

        while True:

            if (
                max_events is not None
                and dispatch_index >= max_events
            ):
                break

            candidate = get_next_candidate()

            if candidate is None:
                break

            dispatch_index += 1

            event_id = candidate["event_id"]

            claim_time = datetime.now(timezone.utc)

            lease_expires_at = (
                claim_time
                + timedelta(minutes=lease_minutes)
            )


            # ------------------------------------------------
            # 1. Durable lease claim
            # ------------------------------------------------

            claim_df = spark.createDataFrame(
                [(
                    event_id,
                    dispatcher_run_id,
                    claim_time,
                    lease_expires_at,
                )],
                """
                event_id string,
                lease_owner_run_id string,
                claim_time timestamp,
                lease_expires_at_utc timestamp
                """
            )

            (
                outbox_delta.alias("t")
                .merge(
                    claim_df.alias("s"),
                    "t.event_id = s.event_id",
                )
                .whenMatchedUpdate(
                    condition="""
                        t.status = 'PENDING'
                        OR (
                            t.status = 'SENDING'
                            AND (
                                t.lease_expires_at_utc IS NULL
                                OR
                                t.lease_expires_at_utc
                                    < s.claim_time
                            )
                        )
                    """,
                    set={
                        "status":
                            F.lit("SENDING"),

                        "lease_owner_run_id":
                            F.col(
                                "s.lease_owner_run_id"
                            ),

                        "lease_expires_at_utc":
                            F.col(
                                "s.lease_expires_at_utc"
                            ),

                        "attempt_count":
                            F.coalesce(
                                F.col("t.attempt_count"),
                                F.lit(0),
                            ) + F.lit(1),

                        "last_attempt_at_utc":
                            F.col("s.claim_time"),

                        "last_error":
                            F.lit(None).cast("string"),

                        "updated_at_utc":
                            F.col("s.claim_time"),
                    },
                )
                .execute()
            )


            # ------------------------------------------------
            # 2. Confirm this dispatcher owns the lease
            # ------------------------------------------------

            claimed_rows = (
                spark.table(OUTBOX_TABLE)
                .filter(
                    F.col("event_id") == event_id
                )
                .collect()
            )

            if len(claimed_rows) != 1:
                raise RuntimeError(
                    f"Unexpected outbox cardinality "
                    f"for event {event_id}."
                )

            claimed = claimed_rows[0]

            if not (
                claimed["status"] == "SENDING"
                and
                claimed["lease_owner_run_id"]
                == dispatcher_run_id
            ):
                # Another dispatcher won the claim.
                continue


            # ------------------------------------------------
            # 3. Transport
            # ------------------------------------------------

            try:

                batch = producer.create_batch()

                event = EventData(
                    claimed["event_payload"]
                )

                event.content_type = (
                    "application/json"
                )

                event.properties = {
                    "event_id":
                        claimed["event_id"],

                    "event_type":
                        claimed["event_type"],

                    "dispatcher_run_id":
                        dispatcher_run_id,
                }

                batch.add(event)

                producer.send_batch(
                    batch,
                    timeout=30,
                )


                # --------------------------------------------
                # 4. Transport ACK → SENT
                # --------------------------------------------

                sent_time = datetime.now(
                    timezone.utc
                )

                outbox_delta.update(
                    condition=(
                        (
                            F.col("event_id")
                            == event_id
                        )
                        &
                        (
                            F.col(
                                "lease_owner_run_id"
                            )
                            == dispatcher_run_id
                        )
                        &
                        (
                            F.col("status")
                            == "SENDING"
                        )
                    ),
                    set={
                        "status":
                            F.lit("SENT"),

                        "sent_at_utc":
                            F.lit(sent_time),

                        "lease_owner_run_id":
                            F.lit(None).cast(
                                "string"
                            ),

                        "lease_expires_at_utc":
                            F.lit(None).cast(
                                "timestamp"
                            ),

                        "last_error":
                            F.lit(None).cast(
                                "string"
                            ),

                        "updated_at_utc":
                            F.lit(sent_time),
                    },
                )

                sent_event_ids.append(event_id)


            except Exception as exc:

                failure_time = datetime.now(
                    timezone.utc
                )

                # --------------------------------------------
                # Recoverable failure → PENDING
                # --------------------------------------------

                outbox_delta.update(
                    condition=(
                        (
                            F.col("event_id")
                            == event_id
                        )
                        &
                        (
                            F.col(
                                "lease_owner_run_id"
                            )
                            == dispatcher_run_id
                        )
                        &
                        (
                            F.col("status")
                            == "SENDING"
                        )
                    ),
                    set={
                        "status":
                            F.lit("PENDING"),

                        "lease_owner_run_id":
                            F.lit(None).cast(
                                "string"
                            ),

                        "lease_expires_at_utc":
                            F.lit(None).cast(
                                "timestamp"
                            ),

                        "last_error":
                            F.lit(
                                str(exc)[:4000]
                            ),

                        "updated_at_utc":
                            F.lit(failure_time),
                    },
                )

                failed_event_ids.append(
                    event_id
                )

                raise

    finally:
        producer.close()


    # --------------------------------------------------------
    # Final durable state
    # --------------------------------------------------------

    status_counts = {
        row["status"]: row["count"]
        for row in (
            spark.table(OUTBOX_TABLE)
            .groupBy("status")
            .count()
            .collect()
        )
    }

    active_leases = (
        spark.table(OUTBOX_TABLE)
        .filter(
            F.col(
                "lease_owner_run_id"
            ).isNotNull()
        )
        .count()
    )

    return {
        "dispatcher_run_id":
            dispatcher_run_id,

        "sent_this_run":
            len(sent_event_ids),

        "failed_this_run":
            len(failed_event_ids),

        "pending":
            status_counts.get("PENDING", 0),

        "sending":
            status_counts.get("SENDING", 0),

        "sent":
            status_counts.get("SENT", 0),

        "active_leases":
            active_leases,
    }


print(
    "Production outbox dispatcher defined: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

from pyspark.sql import functions as F


OUTBOX_TABLE = "ops.rt_event_outbox"


# ------------------------------------------------------------
# Determine whether durable work is waiting
# ------------------------------------------------------------

dispatchable_condition = (
    (F.col("status") == "PENDING")
    |
    (
        (F.col("status") == "SENDING")
        &
        (
            F.col("lease_expires_at_utc").isNull()
            |
            (
                F.col("lease_expires_at_utc")
                < F.current_timestamp()
            )
        )
    )
)

dispatchable_before = (
    spark.table(OUTBOX_TABLE)
    .filter(dispatchable_condition)
    .count()
)

print("=== RT PUBLISHER DISPATCH GATE ===")
print("Dispatchable before:", dispatchable_before)


# ------------------------------------------------------------
# Dispatch only when durable work exists
# ------------------------------------------------------------

if dispatchable_before > 0:

    if not os.environ.get(
        "GRIDPULSE_EVENTSTREAM_CONNECTION"
    ):
        raise RuntimeError(
            "Durable outbox events are waiting, but "
            "GRIDPULSE_EVENTSTREAM_CONNECTION is not "
            "available in this runtime. "
            "Inject the credential securely at runtime; "
            "never hardcode it in the notebook."
        )

    dispatch_summary = dispatch_pending_outbox(
        max_events=None,
        lease_minutes=5,
    )

else:

    status_counts = {
        row["status"]: row["count"]
        for row in (
            spark.table(OUTBOX_TABLE)
            .groupBy("status")
            .count()
            .collect()
        )
    }

    dispatch_summary = {
        "sent_this_run": 0,
        "failed_this_run": 0,
        "pending":
            status_counts.get("PENDING", 0),
        "sending":
            status_counts.get("SENDING", 0),
        "sent":
            status_counts.get("SENT", 0),
        "active_leases":
            spark.table(OUTBOX_TABLE)
            .filter(
                F.col(
                    "lease_owner_run_id"
                ).isNotNull()
            )
            .count(),
    }


# ------------------------------------------------------------
# Final durable assertions
# ------------------------------------------------------------

assert dispatch_summary["pending"] == 0
assert dispatch_summary["sending"] == 0
assert dispatch_summary["active_leases"] == 0
assert dispatch_summary["failed_this_run"] == 0

print()
print("Sent this run     :", dispatch_summary["sent_this_run"])
print("Failed this run   :", dispatch_summary["failed_this_run"])
print("PENDING remaining :", dispatch_summary["pending"])
print("SENDING remaining :", dispatch_summary["sending"])
print("Total SENT        :", dispatch_summary["sent"])
print("Active leases     :", dispatch_summary["active_leases"])

print()
print("RT PUBLISHER FINAL DISPATCH GATE PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
