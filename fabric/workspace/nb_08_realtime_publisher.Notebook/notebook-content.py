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

import hashlib
import importlib
import json
import os
import uuid
import xml.etree.ElementTree as ET

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from azure.eventhub import EventData, EventHubProducerClient
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

import builtin.gridpulse.orchestration as orchestration
import builtin.gridpulse.parsers.price_realtime as rt_parser
from builtin.gridpulse.bronze import compute_file_sha256


# ============================================================
# RUNTIME
# ============================================================

spark.conf.set("spark.sql.session.timeZone", "UTC")

importlib.invalidate_caches()
orchestration = importlib.reload(orchestration)
rt_parser = importlib.reload(rt_parser)


# ============================================================
# SOURCE CONTRACT
# ============================================================

SOURCE_NAME = "ieso_realtime_ontario_zonal_price"
SOURCE_FILE = "PUB_RealtimeOntarioZonalPrice.xml"

SOURCE_URL = (
    "https://reports-public.ieso.ca/public/"
    "RealtimeOntarioZonalPrice/"
    "PUB_RealtimeOntarioZonalPrice.xml"
)


# ============================================================
# DURABLE STATE
# ============================================================

REGISTRY_TABLE = "ops.source_file_registry"
ETL_RUN_TABLE = "ops.etl_run"
OUTBOX_TABLE = "ops.rt_event_outbox"
CHECKPOINT_TABLE = "ops.rt_publisher_checkpoint"


# ============================================================
# EVENT CONTRACT v1
# ============================================================

EVENT_SCHEMA_VERSION = "1.0"

EVENT_TYPE_OBSERVATION = "RT_PRICE_OBSERVATION"
EVENT_TYPE_INVALIDATION = "RT_PRICE_INVALIDATION"

PUBLICATION_STATE_ELIGIBLE = "ELIGIBLE"
PUBLICATION_STATE_INVALIDATED = "INVALIDATED"

NULL_TOKEN = "<NULL>"

PUBLISHABLE_DECISIONS = {
    "NEW_OBSERVATION",
    "REVISED_OBSERVATION",
    "INVALIDATION",
}


# ============================================================
# CANONICALIZATION
# ============================================================

def canonical_decimal(value):
    if value is None:
        return NULL_TOKEN

    decimal_value = Decimal(str(value))

    if decimal_value == 0:
        return "0"

    canonical = format(
        decimal_value.normalize(),
        "f",
    )

    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")

    return canonical


def canonical_nullable_string(value):
    if value is None:
        return NULL_TOKEN

    return str(value)


def build_observation_state(row):
    """
    Semantic interval state.

    Source revision and publisher metadata are intentionally
    excluded so observation_hash changes only when the market
    observation itself changes.
    """

    return {
        "delivery_date":
            str(row["delivery_date"]),

        "delivery_hour":
            int(row["delivery_hour"]),

        "interval":
            int(row["interval"]),

        "zonal_price_capped_cad_per_mwh":
            canonical_decimal(
                row["zonal_price_capped_cad_per_mwh"]
            ),

        "loss_price_capped_cad_per_mwh":
            canonical_decimal(
                row["loss_price_capped_cad_per_mwh"]
            ),

        "congestion_price_capped_cad_per_mwh":
            canonical_decimal(
                row["congestion_price_capped_cad_per_mwh"]
            ),

        "source_flag":
            canonical_nullable_string(
                row["source_flag"]
            ),
    }


def compute_observation_hash(row):

    canonical_json = json.dumps(
        build_observation_state(row),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def compute_event_id(
    delivery_date,
    delivery_hour,
    interval,
    source_created_at_raw,
    source_hash,
):
    """
    Deterministic event identity.

    Retry metadata is deliberately excluded.
    """

    identity = "|".join([
        "rt_price_event_v1",
        str(delivery_date),
        str(int(delivery_hour)),
        str(int(interval)),
        canonical_nullable_string(
            source_created_at_raw
        ),
        source_hash,
    ])

    return hashlib.sha256(
        identity.encode("utf-8")
    ).hexdigest()


# ============================================================
# SERIALIZATION
# ============================================================

def json_safe(value):

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            key: json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            json_safe(item)
            for item in value
        ]

    return value


def serialize_event(event):

    return json.dumps(
        json_safe(event),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def iso_utc(value):

    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(
            tzinfo=timezone.utc
        )

    return value.astimezone(
        timezone.utc
    ).isoformat()


# ============================================================
# XML HELPERS
# ============================================================

def xml_local_name(tag):
    return tag.split("}", 1)[-1]


def find_xml_text(root, element_name):

    for element in root.iter():

        if (
            xml_local_name(element.tag)
            == element_name
        ):

            if element.text is None:
                return None

            value = element.text.strip()

            return value if value else None

    return None


print(
    "GridPulse real-time publisher "
    "production runtime initialized."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DURABLE RT STATE — CREATE / VALIDATE
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS ops")


# ------------------------------------------------------------
# 1. Detect existing tables
# ------------------------------------------------------------

ops_tables = {
    row["tableName"]
    for row in spark.sql(
        "SHOW TABLES IN ops"
    ).collect()
}


# ------------------------------------------------------------
# 2. Publisher checkpoint
# ------------------------------------------------------------

if "rt_publisher_checkpoint" not in ops_tables:

    spark.sql(
        """
        CREATE TABLE ops.rt_publisher_checkpoint (
            source_name STRING,
            last_completed_source_hash STRING,
            last_completed_bronze_path STRING,
            last_completed_first_seen_at_utc TIMESTAMP,
            last_completed_at_utc TIMESTAMP,
            last_successful_poll_at_utc TIMESTAMP,
            last_publisher_run_id STRING,
            updated_at_utc TIMESTAMP
        )
        USING DELTA
        """
    )

    print(
        "Created ops.rt_publisher_checkpoint"
    )


# ------------------------------------------------------------
# 3. Durable event outbox
# ------------------------------------------------------------

if "rt_event_outbox" not in ops_tables:

    spark.sql(
        """
        CREATE TABLE ops.rt_event_outbox (
            event_id STRING,
            event_schema_version STRING,
            event_type STRING,
            publication_state STRING,

            delivery_date DATE,
            delivery_hour INT,
            interval INT,

            observation_hash STRING,
            previous_event_id STRING,

            source_hash STRING,
            bronze_path STRING,
            event_payload STRING,

            status STRING,
            attempt_count BIGINT,

            lease_owner_run_id STRING,
            lease_expires_at_utc TIMESTAMP,

            last_attempt_at_utc TIMESTAMP,
            sent_at_utc TIMESTAMP,
            last_error STRING,

            publisher_run_id STRING,
            poll_id STRING,

            created_at_utc TIMESTAMP,
            updated_at_utc TIMESTAMP
        )
        USING DELTA
        """
    )

    print(
        "Created ops.rt_event_outbox"
    )


# ------------------------------------------------------------
# 4. Validate checkpoint contract
# ------------------------------------------------------------

expected_checkpoint_columns = [
    "source_name",
    "last_completed_source_hash",
    "last_completed_bronze_path",
    "last_completed_first_seen_at_utc",
    "last_completed_at_utc",
    "last_successful_poll_at_utc",
    "last_publisher_run_id",
    "updated_at_utc",
]

checkpoint_df = spark.table(
    CHECKPOINT_TABLE
)

assert checkpoint_df.columns == (
    expected_checkpoint_columns
), (
    "Unexpected checkpoint schema.\n"
    f"Expected: {expected_checkpoint_columns}\n"
    f"Actual:   {checkpoint_df.columns}"
)


# ------------------------------------------------------------
# 5. Validate outbox contract
# ------------------------------------------------------------

expected_outbox_columns = [
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
]

outbox_df = spark.table(
    OUTBOX_TABLE
)

assert outbox_df.columns == (
    expected_outbox_columns
), (
    "Unexpected outbox schema.\n"
    f"Expected: {expected_outbox_columns}\n"
    f"Actual:   {outbox_df.columns}"
)


# ------------------------------------------------------------
# 6. Validate critical data types
# ------------------------------------------------------------

outbox_types = {
    field.name: field.dataType.simpleString()
    for field in outbox_df.schema.fields
}

checkpoint_types = {
    field.name: field.dataType.simpleString()
    for field in checkpoint_df.schema.fields
}

assert outbox_types["event_id"] == "string"
assert outbox_types["delivery_date"] == "date"
assert outbox_types["delivery_hour"] == "int"
assert outbox_types["interval"] == "int"
assert outbox_types["event_payload"] == "string"
assert outbox_types["status"] == "string"

# Existing deployments may use int or bigint for attempt_count.
assert outbox_types["attempt_count"] in {
    "int",
    "bigint",
}

assert (
    outbox_types["lease_expires_at_utc"]
    == "timestamp"
)

assert (
    outbox_types["created_at_utc"]
    == "timestamp"
)

assert (
    checkpoint_types[
        "last_completed_at_utc"
    ]
    == "timestamp"
)


# ------------------------------------------------------------
# 7. Current durable-state summary
# ------------------------------------------------------------

checkpoint_rows = checkpoint_df.count()
outbox_rows = outbox_df.count()

print(
    "RT durable tables validated."
)
print(
    "Checkpoint rows:",
    checkpoint_rows,
)
print(
    "Outbox rows     :",
    outbox_rows,
)
print(
    "STEP 55.4B DURABLE STATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SINGLE SOURCE POLL → EXACT BRONZE EVIDENCE
# ============================================================

print("=== GRIDPULSE RT SOURCE POLL ===")


# ------------------------------------------------------------
# 1. ONE HTTP acquisition per notebook execution
# ------------------------------------------------------------

poll_result = orchestration.ingest_http_source(
    spark=spark,
    source_name=SOURCE_NAME,
    source_url=SOURCE_URL,
    source_file=SOURCE_FILE,
    logical_key_kwargs={
        "source_file": SOURCE_FILE,
    },
)

poll_id = poll_result["run_id"]
current_source_hash = poll_result["source_hash"]
current_bronze_path = poll_result["bronze_path"]
source_classification = poll_result["classification"]


assert poll_id
assert current_source_hash
assert current_bronze_path


# ------------------------------------------------------------
# 2. Read EXACT persisted Bronze bytes
#
# No second HTTP request is allowed below this point.
# ------------------------------------------------------------

local_bronze_path = (
    f"/lakehouse/default/"
    f"{current_bronze_path}"
)

with open(local_bronze_path, "rb") as f:
    current_payload_bytes = f.read()

assert len(current_payload_bytes) > 0


# ------------------------------------------------------------
# 3. Exact SHA-256 validation
# ------------------------------------------------------------

persisted_sha256 = hashlib.sha256(
    current_payload_bytes
).hexdigest()

assert (
    persisted_sha256
    == current_source_hash
), (
    "Persisted Bronze bytes do not match "
    "the source_hash returned by ingestion."
)


# ------------------------------------------------------------
# 4. Registry lineage validation
# ------------------------------------------------------------

registry_matches = (
    spark.table(REGISTRY_TABLE)
    .filter(
        (F.col("source_name") == SOURCE_NAME)
        &
        (
            F.col("source_hash")
            == current_source_hash
        )
    )
    .collect()
)

assert len(registry_matches) == 1, (
    "Expected exactly one source registry row "
    "for the current source hash."
)

current_registry = registry_matches[0]

assert (
    current_registry["bronze_path"]
    == current_bronze_path
)

assert (
    current_registry["processing_status"]
    == "SUCCESS"
)

assert (
    current_registry["file_size"]
    == len(current_payload_bytes)
)


# ------------------------------------------------------------
# 5. Operational lineage
# ------------------------------------------------------------

bronze_first_seen_at_utc = (
    current_registry["first_seen_timestamp"]
)

if (
    bronze_first_seen_at_utc is not None
    and bronze_first_seen_at_utc.tzinfo is None
):
    bronze_first_seen_at_utc = (
        bronze_first_seen_at_utc.replace(
            tzinfo=timezone.utc
        )
    )


# ------------------------------------------------------------
# 6. Run summary
# ------------------------------------------------------------

print("Classification        :", source_classification)
print("Poll ID               :", poll_id)
print("Source hash           :", current_source_hash)
print("Bronze path           :", current_bronze_path)
print("Persisted bytes       :", len(current_payload_bytes))
print("SHA-256 match         :", True)
print(
    "Bronze first seen UTC:",
    bronze_first_seen_at_utc,
)

print()
print(
    "HTTP acquisitions this run: 1"
)
print(
    "STEP 55.4C SINGLE POLL / BRONZE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CHECKPOINT GATE + SOURCE XML METADATA
# ============================================================

print("=== GRIDPULSE RT CHECKPOINT GATE ===")


# ------------------------------------------------------------
# 1. Read current durable checkpoint
# ------------------------------------------------------------

checkpoint_matches = (
    spark.table(CHECKPOINT_TABLE)
    .filter(
        F.col("source_name") == SOURCE_NAME
    )
    .collect()
)

assert len(checkpoint_matches) <= 1, (
    "Multiple publisher checkpoint rows found "
    f"for source_name={SOURCE_NAME}."
)

if checkpoint_matches:

    current_checkpoint = checkpoint_matches[0]

    previous_completed_source_hash = (
        current_checkpoint[
            "last_completed_source_hash"
        ]
    )

else:

    current_checkpoint = None
    previous_completed_source_hash = None


# ------------------------------------------------------------
# 2. Completion gate
#
# IMPORTANT:
# We compare against the durable publisher checkpoint,
# not only the Bronze registry classification.
#
# A source file can already exist in Bronze while still
# requiring publication if the publisher checkpoint lags.
# ------------------------------------------------------------

has_unprocessed_revision = (
    previous_completed_source_hash
    != current_source_hash
)

if previous_completed_source_hash is None:
    revision_gate = "BOOTSTRAP"

elif has_unprocessed_revision:
    revision_gate = "NEW_REVISION"

else:
    revision_gate = "UNCHANGED"


# ------------------------------------------------------------
# 3. Parse source metadata from EXACT persisted Bronze bytes
#
# CreatedAt remains a raw source string.
# No timezone inference is allowed.
# ------------------------------------------------------------

xml_root = ET.fromstring(
    current_payload_bytes
)

source_created_at_raw = find_xml_text(
    xml_root,
    "CreatedAt",
)

source_doc_revision = find_xml_text(
    xml_root,
    "DocRevision",
)


# ------------------------------------------------------------
# 4. Record successful poll observability
#
# This does NOT advance:
#   last_completed_source_hash
#   last_completed_bronze_path
#   last_completed_at_utc
#
# Completion advances only after durable outbox processing.
# ------------------------------------------------------------

successful_poll_at_utc = datetime.now(
    timezone.utc
)

checkpoint_delta = DeltaTable.forName(
    spark,
    CHECKPOINT_TABLE,
)

poll_state_df = spark.createDataFrame(
    [(
        SOURCE_NAME,
        successful_poll_at_utc,
    )],
    """
    source_name string,
    successful_poll_at_utc timestamp
    """
)

(
    checkpoint_delta.alias("t")
    .merge(
        poll_state_df.alias("s"),
        "t.source_name = s.source_name",
    )
    .whenMatchedUpdate(
        set={
            "last_successful_poll_at_utc":
                F.col(
                    "s.successful_poll_at_utc"
                ),

            "updated_at_utc":
                F.col(
                    "s.successful_poll_at_utc"
                ),
        },
    )
    .whenNotMatchedInsert(
        values={
            "source_name":
                F.col("s.source_name"),

            "last_completed_source_hash":
                F.lit(None).cast("string"),

            "last_completed_bronze_path":
                F.lit(None).cast("string"),

            "last_completed_first_seen_at_utc":
                F.lit(None).cast("timestamp"),

            "last_completed_at_utc":
                F.lit(None).cast("timestamp"),

            "last_successful_poll_at_utc":
                F.col(
                    "s.successful_poll_at_utc"
                ),

            "last_publisher_run_id":
                F.lit(None).cast("string"),

            "updated_at_utc":
                F.col(
                    "s.successful_poll_at_utc"
                ),
        },
    )
    .execute()
)


# ------------------------------------------------------------
# 5. Validate that completion state was NOT advanced
# ------------------------------------------------------------

checkpoint_after_poll = (
    spark.table(CHECKPOINT_TABLE)
    .filter(
        F.col("source_name") == SOURCE_NAME
    )
    .collect()
)

assert len(checkpoint_after_poll) == 1

checkpoint_after_poll = (
    checkpoint_after_poll[0]
)

assert (
    checkpoint_after_poll[
        "last_completed_source_hash"
    ]
    == previous_completed_source_hash
), (
    "Poll observability update unexpectedly "
    "advanced the completed source hash."
)


# ------------------------------------------------------------
# 6. Gate summary
# ------------------------------------------------------------

print(
    "Bronze classification   :",
    source_classification,
)

print(
    "Previous completed hash :",
    previous_completed_source_hash,
)

print(
    "Current source hash     :",
    current_source_hash,
)

print(
    "Revision gate           :",
    revision_gate,
)

print(
    "Needs processing        :",
    has_unprocessed_revision,
)

print(
    "CreatedAt raw           :",
    source_created_at_raw,
)

print(
    "DocRevision             :",
    source_doc_revision,
)

print(
    "Successful poll UTC     :",
    successful_poll_at_utc,
)

print()
print(
    "Completed checkpoint advanced: NO"
)
print(
    "STEP 55.4D CHECKPOINT GATE PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# REVISION-AWARE PROCESSING → DURABLE OUTBOX
# ============================================================

print("=== GRIDPULSE RT REVISION PROCESSING ===")

publisher_run_id = str(uuid.uuid4())
processing_time_utc = datetime.now(timezone.utc)

new_event_records = []
decision_summary = {
    "UNCHANGED": 0,
    "NEW_OBSERVATION": 0,
    "REVISED_OBSERVATION": 0,
    "INVALIDATION": 0,
    "SUPPRESSED_INELIGIBLE": 0,
}


# ------------------------------------------------------------
# 1. Process only when checkpoint says this revision is new
# ------------------------------------------------------------

if has_unprocessed_revision:

    # --------------------------------------------------------
    # Parse EXACT persisted Bronze.
    # No second HTTP request.
    # --------------------------------------------------------

    raw_df = rt_parser.parse_realtime_price(
        spark=spark,
        bronze_path=current_bronze_path,
    )

    typed_df = (
        rt_parser
        .type_realtime_price(raw_df)
        .cache()
    )


    # --------------------------------------------------------
    # Structural contract
    # --------------------------------------------------------

    required_columns = {
        "delivery_date",
        "delivery_hour",
        "interval",
        "zonal_price_capped_cad_per_mwh",
        "loss_price_capped_cad_per_mwh",
        "congestion_price_capped_cad_per_mwh",
        "source_flag",
    }

    missing_columns = (
        required_columns
        - set(typed_df.columns)
    )

    assert not missing_columns, (
        "Realtime parser contract mismatch. "
        f"Missing columns: {sorted(missing_columns)}"
    )

    parsed_rows = typed_df.count()

    duplicate_keys = (
        typed_df
        .groupBy(
            "delivery_date",
            "delivery_hour",
            "interval",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert parsed_rows == 12, (
        f"Expected 12 interval rows, found {parsed_rows}."
    )

    assert duplicate_keys == 0, (
        "Duplicate realtime business keys detected."
    )

    intervals = sorted(
        row["interval"]
        for row in (
            typed_df
            .select("interval")
            .distinct()
            .collect()
        )
    )

    assert intervals == list(range(1, 13))


    # --------------------------------------------------------
    # Population state
    # --------------------------------------------------------

    all_prices_present = (
        F.col(
            "zonal_price_capped_cad_per_mwh"
        ).isNotNull()
        &
        F.col(
            "loss_price_capped_cad_per_mwh"
        ).isNotNull()
        &
        F.col(
            "congestion_price_capped_cad_per_mwh"
        ).isNotNull()
    )

    all_prices_missing = (
        F.col(
            "zonal_price_capped_cad_per_mwh"
        ).isNull()
        &
        F.col(
            "loss_price_capped_cad_per_mwh"
        ).isNull()
        &
        F.col(
            "congestion_price_capped_cad_per_mwh"
        ).isNull()
    )

    state_df = (
        typed_df
        .withColumn(
            "population_state",
            F.when(
                all_prices_present,
                F.lit("FULLY_POPULATED"),
            )
            .when(
                all_prices_missing,
                F.lit("FULLY_EMPTY"),
            )
            .otherwise(
                F.lit("PARTIALLY_POPULATED"),
            ),
        )
    )


    # --------------------------------------------------------
    # Latest durable logical publication per business key
    # --------------------------------------------------------

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

    latest_rows = (
        spark.table(OUTBOX_TABLE)
        .withColumn(
            "_rn",
            F.row_number().over(
                latest_window
            ),
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
        for row in latest_rows
    }


    # --------------------------------------------------------
    # Decision + event construction
    # --------------------------------------------------------

    fresh_rows = (
        state_df
        .orderBy(
            "delivery_date",
            "delivery_hour",
            "interval",
        )
        .collect()
    )

    for row in fresh_rows:

        key = (
            str(row["delivery_date"]),
            int(row["delivery_hour"]),
            int(row["interval"]),
        )

        previous = latest_by_key.get(key)

        previous_event_id = (
            previous["event_id"]
            if previous is not None
            else None
        )

        previous_publication_state = (
            previous["publication_state"]
            if previous is not None
            else None
        )

        previous_observation_hash = (
            previous["observation_hash"]
            if previous is not None
            else None
        )

        observation_hash = (
            compute_observation_hash(row)
        )

        population_state = (
            row["population_state"]
        )


        # ----------------------------------------------------
        # Revision semantics
        # ----------------------------------------------------

        if (
            population_state
            == "FULLY_POPULATED"
        ):

            if (
                previous is not None
                and
                previous_publication_state
                == PUBLICATION_STATE_ELIGIBLE
                and
                previous_observation_hash
                == observation_hash
            ):
                decision = "UNCHANGED"

            elif (
                previous is not None
                and
                previous_publication_state
                == PUBLICATION_STATE_ELIGIBLE
            ):
                decision = (
                    "REVISED_OBSERVATION"
                )

            else:
                decision = "NEW_OBSERVATION"

        else:

            if (
                previous is not None
                and
                previous_publication_state
                == PUBLICATION_STATE_ELIGIBLE
            ):
                decision = "INVALIDATION"

            else:
                decision = (
                    "SUPPRESSED_INELIGIBLE"
                )


        decision_summary[decision] += 1


        # ----------------------------------------------------
        # No publication required
        # ----------------------------------------------------

        if decision in {
            "UNCHANGED",
            "SUPPRESSED_INELIGIBLE",
        }:
            continue


        # ----------------------------------------------------
        # Event identity
        # ----------------------------------------------------

        event_id = compute_event_id(
            delivery_date=row[
                "delivery_date"
            ],
            delivery_hour=row[
                "delivery_hour"
            ],
            interval=row["interval"],
            source_created_at_raw=(
                source_created_at_raw
            ),
            source_hash=current_source_hash,
        )


        # ----------------------------------------------------
        # Observation vs invalidation contract
        # ----------------------------------------------------

        if decision == "INVALIDATION":

            event_type = (
                EVENT_TYPE_INVALIDATION
            )

            publication_state = (
                PUBLICATION_STATE_INVALIDATED
            )

            event_previous_id = (
                previous_event_id
            )

        else:

            event_type = (
                EVENT_TYPE_OBSERVATION
            )

            publication_state = (
                PUBLICATION_STATE_ELIGIBLE
            )

            # v1 uses previous_event_id primarily for
            # explicit invalidation lineage.
            event_previous_id = None


        # ----------------------------------------------------
        # Event payload
        # ----------------------------------------------------

        event_payload = {
            "event_schema_version":
                EVENT_SCHEMA_VERSION,

            "event_type":
                event_type,

            "event_id":
                event_id,

            "observation_hash":
                observation_hash,

            "delivery_date":
                str(row["delivery_date"]),

            "delivery_hour":
                int(row["delivery_hour"]),

            "interval":
                int(row["interval"]),

            "zonal_price_capped_cad_per_mwh":
                (
                    float(
                        row[
                            "zonal_price_capped_cad_per_mwh"
                        ]
                    )
                    if row[
                        "zonal_price_capped_cad_per_mwh"
                    ] is not None
                    else None
                ),

            "loss_price_capped_cad_per_mwh":
                (
                    float(
                        row[
                            "loss_price_capped_cad_per_mwh"
                        ]
                    )
                    if row[
                        "loss_price_capped_cad_per_mwh"
                    ] is not None
                    else None
                ),

            "congestion_price_capped_cad_per_mwh":
                (
                    float(
                        row[
                            "congestion_price_capped_cad_per_mwh"
                        ]
                    )
                    if row[
                        "congestion_price_capped_cad_per_mwh"
                    ] is not None
                    else None
                ),

            "source_flag":
                row["source_flag"],

            "publication_state":
                publication_state,

            "previous_event_id":
                event_previous_id,

            "source_name":
                current_registry[
                    "source_name"
                ],

            "source_file":
                current_registry[
                    "source_file"
                ],

            "source_url":
                current_registry[
                    "source_url"
                ],

            "source_hash":
                current_source_hash,

            "source_created_at_raw":
                source_created_at_raw,

            "source_doc_revision":
                source_doc_revision,

            "bronze_first_seen_at_utc":
                iso_utc(
                    bronze_first_seen_at_utc
                ),

            "publisher_run_id":
                publisher_run_id,

            "poll_id":
                poll_id,

            "event_created_at_utc":
                processing_time_utc.isoformat(),
        }


        new_event_records.append({
            "event_id":
                event_id,

            "event_schema_version":
                EVENT_SCHEMA_VERSION,

            "event_type":
                event_type,

            "publication_state":
                publication_state,

            "delivery_date":
                row["delivery_date"],

            "delivery_hour":
                int(row["delivery_hour"]),

            "interval":
                int(row["interval"]),

            "observation_hash":
                observation_hash,

            "previous_event_id":
                event_previous_id,

            "source_hash":
                current_source_hash,

            "bronze_path":
                current_bronze_path,

            "event_payload":
                serialize_event(
                    event_payload
                ),

            "status":
                "PENDING",

            "attempt_count":
                0,

            "lease_owner_run_id":
                None,

            "lease_expires_at_utc":
                None,

            "last_attempt_at_utc":
                None,

            "sent_at_utc":
                None,

            "last_error":
                None,

            "publisher_run_id":
                publisher_run_id,

            "poll_id":
                poll_id,

            "created_at_utc":
                processing_time_utc,

            "updated_at_utc":
                processing_time_utc,
        })


    # --------------------------------------------------------
    # Durable idempotent outbox MERGE
    # --------------------------------------------------------

    outbox_schema = spark.table(
        OUTBOX_TABLE
    ).schema

    events_df = spark.createDataFrame(
        new_event_records,
        schema=outbox_schema,
    )

    if new_event_records:

        outbox_delta = DeltaTable.forName(
            spark,
            OUTBOX_TABLE,
        )

        (
            outbox_delta.alias("t")
            .merge(
                events_df.alias("s"),
                "t.event_id = s.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


    # --------------------------------------------------------
    # Validate the revision is durably represented
    # before advancing completion checkpoint.
    # --------------------------------------------------------

    expected_event_ids = {
        event["event_id"]
        for event in new_event_records
    }

    if expected_event_ids:

        persisted_event_ids = {
            row["event_id"]
            for row in (
                spark.table(
                    OUTBOX_TABLE
                )
                .filter(
                    F.col("source_hash")
                    == current_source_hash
                )
                .select("event_id")
                .collect()
            )
        }

        assert expected_event_ids.issubset(
            persisted_event_ids
        ), (
            "Not all publishable events were "
            "persisted to the durable outbox."
        )


    # --------------------------------------------------------
    # Advance COMPLETED checkpoint only now
    # --------------------------------------------------------

    completed_at_utc = datetime.now(
        timezone.utc
    )

    completed_df = spark.createDataFrame(
        [(
            SOURCE_NAME,
            current_source_hash,
            current_bronze_path,
            bronze_first_seen_at_utc,
            completed_at_utc,
            successful_poll_at_utc,
            publisher_run_id,
            completed_at_utc,
        )],
        """
        source_name string,
        last_completed_source_hash string,
        last_completed_bronze_path string,
        last_completed_first_seen_at_utc timestamp,
        last_completed_at_utc timestamp,
        last_successful_poll_at_utc timestamp,
        last_publisher_run_id string,
        updated_at_utc timestamp
        """,
    )

    checkpoint_delta = DeltaTable.forName(
        spark,
        CHECKPOINT_TABLE,
    )

    (
        checkpoint_delta.alias("t")
        .merge(
            completed_df.alias("s"),
            "t.source_name = s.source_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


else:

    # --------------------------------------------------------
    # UNCHANGED source:
    # poll observability was already updated in 55.4D.
    # No parser/outbox/completion mutation required.
    # --------------------------------------------------------

    parsed_rows = 0


# ------------------------------------------------------------
# 2. Final processing validation
# ------------------------------------------------------------

checkpoint_final = (
    spark.table(CHECKPOINT_TABLE)
    .filter(
        F.col("source_name") == SOURCE_NAME
    )
    .collect()
)

assert len(checkpoint_final) == 1

checkpoint_final = checkpoint_final[0]

assert (
    checkpoint_final[
        "last_completed_source_hash"
    ]
    == current_source_hash
), (
    "Completed checkpoint does not match "
    "the current processed revision."
)


# ------------------------------------------------------------
# 3. Summary
# ------------------------------------------------------------

print(
    "Revision gate           :",
    revision_gate,
)

print(
    "Parsed interval rows    :",
    parsed_rows,
)

print(
    "UNCHANGED               :",
    decision_summary["UNCHANGED"],
)

print(
    "NEW_OBSERVATION         :",
    decision_summary[
        "NEW_OBSERVATION"
    ],
)

print(
    "REVISED_OBSERVATION     :",
    decision_summary[
        "REVISED_OBSERVATION"
    ],
)

print(
    "INVALIDATION            :",
    decision_summary[
        "INVALIDATION"
    ],
)

print(
    "SUPPRESSED_INELIGIBLE   :",
    decision_summary[
        "SUPPRESSED_INELIGIBLE"
    ],
)

print(
    "Events prepared         :",
    len(new_event_records),
)

print(
    "Completed checkpoint    :",
    checkpoint_final[
        "last_completed_source_hash"
    ],
)

print()
print(
    "STEP 55.4E REVISION PROCESSING PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# DURABLE OUTBOX DISPATCHER
# ============================================================

def dispatch_pending_outbox(
    max_events=None,
    lease_minutes=5,
):
    """
    Send durable realtime events to Fabric Eventstream.

    Eligible events:
      - PENDING
      - SENDING with an expired lease

    Delivery model:
      - durable claim before transport
      - SENT only after transport ACK
      - failed sends return to PENDING
      - deterministic event_id survives retries
      - duplicate physical delivery is tolerated downstream
    """

    connection_string = os.environ.get(
        "GRIDPULSE_EVENTSTREAM_CONNECTION"
    )

    if not connection_string:
        raise RuntimeError(
            "GRIDPULSE_EVENTSTREAM_CONNECTION is not "
            "available in this runtime. "
            "Inject the credential securely at runtime; "
            "never hardcode it in the notebook."
        )

    if (
        max_events is not None
        and max_events <= 0
    ):
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
    # Resolve next dispatchable durable event
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
                        F.col(
                            "lease_expires_at_utc"
                        ).isNull()
                        |
                        (
                            F.col(
                                "lease_expires_at_utc"
                            )
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


    producer = (
        EventHubProducerClient
        .from_connection_string(
            conn_str=connection_string
        )
    )

    try:

        processed = 0

        while True:

            if (
                max_events is not None
                and processed >= max_events
            ):
                break

            candidate = get_next_candidate()

            if candidate is None:
                break

            processed += 1

            event_id = candidate["event_id"]

            claim_time = datetime.now(
                timezone.utc
            )

            lease_expires_at = (
                claim_time
                + timedelta(
                    minutes=lease_minutes
                )
            )


            # ------------------------------------------------
            # 1. Durable conditional claim
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
                """,
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
                                F.col(
                                    "t.attempt_count"
                                ),
                                F.lit(0),
                            ) + F.lit(1),

                        "last_attempt_at_utc":
                            F.col("s.claim_time"),

                        "last_error":
                            F.lit(None).cast(
                                "string"
                            ),

                        "updated_at_utc":
                            F.col("s.claim_time"),
                    },
                )
                .execute()
            )


            # ------------------------------------------------
            # 2. Confirm ownership before network send
            # ------------------------------------------------

            claimed_rows = (
                spark.table(OUTBOX_TABLE)
                .filter(
                    F.col("event_id")
                    == event_id
                )
                .collect()
            )

            assert len(claimed_rows) == 1

            claimed = claimed_rows[0]

            if not (
                claimed["status"] == "SENDING"
                and
                claimed[
                    "lease_owner_run_id"
                ] == dispatcher_run_id
            ):
                # Another dispatcher won the lease.
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

                sent_event_ids.append(
                    event_id
                )


            except Exception as exc:

                failure_time = datetime.now(
                    timezone.utc
                )

                # --------------------------------------------
                # Recoverable transport failure
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
                            F.lit(
                                failure_time
                            ),
                    },
                )

                failed_event_ids.append(
                    event_id
                )

                raise

    finally:
        producer.close()


    # --------------------------------------------------------
    # Durable post-dispatch summary
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
            status_counts.get(
                "PENDING",
                0,
            ),

        "sending":
            status_counts.get(
                "SENDING",
                0,
            ),

        "sent":
            status_counts.get(
                "SENT",
                0,
            ),

        "active_leases":
            active_leases,
    }


print(
    "GridPulse durable outbox dispatcher defined."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PRODUCTION DISPATCH GATE + FINAL RUN SUMMARY
# ============================================================

print("=== GRIDPULSE RT DISPATCH GATE ===")


# ------------------------------------------------------------
# 1. Determine currently dispatchable durable events
# ------------------------------------------------------------

dispatchable_before = (
    spark.table(OUTBOX_TABLE)
    .filter(
        (F.col("status") == "PENDING")
        |
        (
            (F.col("status") == "SENDING")
            &
            (
                F.col(
                    "lease_expires_at_utc"
                ).isNull()
                |
                (
                    F.col(
                        "lease_expires_at_utc"
                    )
                    < F.current_timestamp()
                )
            )
        )
    )
    .count()
)


print(
    "Dispatchable before:",
    dispatchable_before,
)


# ------------------------------------------------------------
# 2. Dispatch only when work exists
#
# An UNCHANGED/no-backlog run does not require the
# Eventstream credential.
# ------------------------------------------------------------

if dispatchable_before > 0:

    if not os.environ.get(
        "GRIDPULSE_EVENTSTREAM_CONNECTION"
    ):
        raise RuntimeError(
            f"{dispatchable_before} durable realtime "
            "event(s) require transport, but "
            "GRIDPULSE_EVENTSTREAM_CONNECTION is not "
            "available in this runtime. "
            "Inject the credential securely and rerun "
            "the dispatch stage. "
            "The events remain durable in the outbox."
        )

    dispatch_result = (
        dispatch_pending_outbox(
            max_events=None,
            lease_minutes=5,
        )
    )

else:

    dispatch_result = {
        "dispatcher_run_id": None,
        "sent_this_run": 0,
        "failed_this_run": 0,
    }


# ------------------------------------------------------------
# 3. Durable final-state audit
# ------------------------------------------------------------

final_status_counts = {
    row["status"]: row["count"]
    for row in (
        spark.table(OUTBOX_TABLE)
        .groupBy("status")
        .count()
        .collect()
    )
}

total_outbox = (
    spark.table(OUTBOX_TABLE)
    .count()
)

pending_final = final_status_counts.get(
    "PENDING",
    0,
)

sending_final = final_status_counts.get(
    "SENDING",
    0,
)

sent_final = final_status_counts.get(
    "SENT",
    0,
)

active_leases_final = (
    spark.table(OUTBOX_TABLE)
    .filter(
        F.col(
            "lease_owner_run_id"
        ).isNotNull()
    )
    .count()
)

sent_without_timestamp = (
    spark.table(OUTBOX_TABLE)
    .filter(
        (F.col("status") == "SENT")
        &
        F.col("sent_at_utc").isNull()
    )
    .count()
)


# ------------------------------------------------------------
# 4. Production invariants
# ------------------------------------------------------------

assert pending_final == 0, (
    f"Durable outbox still has "
    f"{pending_final} PENDING event(s)."
)

assert sending_final == 0, (
    f"Durable outbox still has "
    f"{sending_final} SENDING event(s)."
)

assert active_leases_final == 0, (
    f"{active_leases_final} active lease(s) remain."
)

assert sent_without_timestamp == 0, (
    "SENT events without sent_at_utc detected."
)


# ------------------------------------------------------------
# 5. Run summary
# ------------------------------------------------------------

print()
print("=== GRIDPULSE RT RUN SUMMARY ===")

print(
    "Source classification :",
    source_classification,
)

print(
    "Revision gate         :",
    revision_gate,
)

print(
    "Current source hash   :",
    current_source_hash,
)

print(
    "Events prepared       :",
    len(new_event_records),
)

print(
    "Sent this dispatch    :",
    dispatch_result[
        "sent_this_run"
    ],
)

print(
    "Failed this dispatch  :",
    dispatch_result[
        "failed_this_run"
    ],
)

print(
    "Total outbox          :",
    total_outbox,
)

print(
    "Total SENT            :",
    sent_final,
)

print(
    "PENDING               :",
    pending_final,
)

print(
    "SENDING               :",
    sending_final,
)

print(
    "Active leases         :",
    active_leases_final,
)

print()
print(
    "GRIDPULSE REAL-TIME PUBLISHER RUN PASSED."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
