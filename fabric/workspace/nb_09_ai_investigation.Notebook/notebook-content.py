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

# # GridPulse AI — Phase 5 Investigation Harness
# 
# This notebook demonstrates governed, evidence-aware market investigation over GridPulse trusted serving layers.
# 
# ## Design principles
# 
# - Trusted Batch evidence comes from Gold SQL serving objects.
# - Current Real-Time evidence comes from governed Eventhouse KQL functions.
# - Critical KPIs are calculated deterministically, not by the LLM.
# - NULL is never silently converted to zero.
# - Negative electricity price components are valid observations unless a specific DQ rule indicates otherwise.
# - Evidence is classified as:
#   - SUFFICIENT
#   - PARTIAL
#   - INSUFFICIENT
#   - BLOCKED_BY_DQ
# - Correlation is not presented as causality.
# - This notebook is an investigation/evaluation harness, not the production AI runtime.

# CELL ********************

from datetime import datetime, timezone

print("GridPulse Phase 5 Investigation Harness")
print("Started at UTC:", datetime.now(timezone.utc).isoformat())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ============================================================
# MAGIC    GRIDPULSE AI — PHASE 5
# MAGIC    SQL Investigation Probe
# MAGIC    ============================================================ */
# MAGIC 
# MAGIC SELECT
# MAGIC     CAST('SQL_READY' AS VARCHAR(50)) AS status,
# MAGIC     COUNT(*) AS demand_rows
# MAGIC FROM gold.fact_market_demand_hourly;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# KQL Investigation Probe
# ============================================================

from notebookutils import mssparkutils

kusto_uri = "https://trd-bwm1mgx3fpsmq71u2j.z3.kusto.fabric.microsoft.com"
database = "eh_gridpulse_realtime"

kusto_query = """
fn_rt_price_current()
| summarize
    current_rows = count(),
    min_delivery_date = min(delivery_date),
    max_delivery_date = max(delivery_date)
"""

# Microsoft Entra token for Fabric KQL / Eventhouse
access_token = mssparkutils.credentials.getToken("kusto")

kql_probe = (
    spark.read
    .format("com.microsoft.kusto.spark.synapse.datasource")
    .option("accessToken", access_token)
    .option("kustoCluster", kusto_uri)
    .option("kustoDatabase", database)
    .option("kustoQuery", kusto_query)
    .load()
)

display(kql_probe)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Evidence-State Evaluator
# ============================================================

VALID_EVIDENCE_STATES = {
    "SUFFICIENT",
    "PARTIAL",
    "INSUFFICIENT",
    "BLOCKED_BY_DQ",
}


def derive_evidence_state(
    *,
    primary_evidence_exists: bool,
    required_metrics_usable: bool,
    requested_scope_complete: bool,
    blocking_dq_failure: bool,
):
    """
    Deterministically classify investigation evidence.

    Precedence:
    1. Missing primary evidence        -> INSUFFICIENT
    2. Required metrics unusable       -> INSUFFICIENT
    3. Applicable blocking DQ failure  -> BLOCKED_BY_DQ
    4. Incomplete requested scope      -> PARTIAL
    5. Otherwise                       -> SUFFICIENT
    """

    reasons = []

    if not primary_evidence_exists:
        state = "INSUFFICIENT"
        reasons.append("NO_PRIMARY_EVIDENCE")

    elif not required_metrics_usable:
        state = "INSUFFICIENT"
        reasons.append("MISSING_REQUIRED_METRICS")

    elif blocking_dq_failure:
        state = "BLOCKED_BY_DQ"
        reasons.append("DQ_BLOCKING_FAILURE")

    elif not requested_scope_complete:
        state = "PARTIAL"
        reasons.append("INCOMPLETE_COVERAGE")

    else:
        state = "SUFFICIENT"
        reasons.append("EVIDENCE_COMPLETE")

    assert state in VALID_EVIDENCE_STATES

    return {
        "evidence_state": state,
        "reason_codes": reasons,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Evidence-State Policy Tests
# ============================================================

test_cases = [
    {
        "name": "Demand 2026-08-28 full day",
        "inputs": dict(
            primary_evidence_exists=True,
            required_metrics_usable=True,
            requested_scope_complete=True,
            blocking_dq_failure=False,
        ),
        "expected": "SUFFICIENT",
    },
    {
        "name": "Demand 2026-08-29 full day",
        "inputs": dict(
            primary_evidence_exists=True,
            required_metrics_usable=True,
            requested_scope_complete=False,
            blocking_dq_failure=False,
        ),
        "expected": "PARTIAL",
    },
    {
        "name": "Missing Day-Ahead date",
        "inputs": dict(
            primary_evidence_exists=False,
            required_metrics_usable=False,
            requested_scope_complete=False,
            blocking_dq_failure=False,
        ),
        "expected": "INSUFFICIENT",
    },
    {
        "name": "Historical RT row with empty prices",
        "inputs": dict(
            primary_evidence_exists=True,
            required_metrics_usable=False,
            requested_scope_complete=True,
            blocking_dq_failure=False,
        ),
        "expected": "INSUFFICIENT",
    },
    {
        "name": "Trusted row with blocking DQ failure",
        "inputs": dict(
            primary_evidence_exists=True,
            required_metrics_usable=True,
            requested_scope_complete=True,
            blocking_dq_failure=True,
        ),
        "expected": "BLOCKED_BY_DQ",
    },
    {
        "name": "Single-hour demand observation",
        "inputs": dict(
            primary_evidence_exists=True,
            required_metrics_usable=True,
            requested_scope_complete=True,
            blocking_dq_failure=False,
        ),
        "expected": "SUFFICIENT",
    },
]


results = []

for case in test_cases:
    actual = derive_evidence_state(**case["inputs"])

    passed = actual["evidence_state"] == case["expected"]

    results.append(
        {
            "test": case["name"],
            "expected": case["expected"],
            "actual": actual["evidence_state"],
            "reason_codes": ", ".join(actual["reason_codes"]),
            "passed": passed,
        }
    )

    assert passed, (
        f"Evidence test failed: {case['name']} "
        f"expected={case['expected']} "
        f"actual={actual['evidence_state']}"
    )


display(spark.createDataFrame(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Direct SQL Analytics Endpoint Read Probe
# ============================================================

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

direct_sql_probe = (
    spark.read
    .synapsesql(
        "lh_gridpulse.gold.fact_market_demand_hourly"
    )
)

print("row_count =", direct_sql_probe.count())

display(
    direct_sql_probe
    .select(
        "market_date",
        "hour_ending",
        "ontario_demand_mw",
        "market_demand_mw"
    )
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Governed SQL Object Reader
#
# Uses direct SQL Analytics Endpoint object reads.
# Passthrough T-SQL is deliberately avoided in the notebook
# because the current Fabric runtime fails to render it.
# ============================================================

import com.microsoft.spark.fabric
from pyspark.sql import functions as F


def read_gridpulse_sql_object(object_name: str):
    """
    Read a trusted GridPulse SQL Analytics Endpoint object.

    Example:
        gold.fact_market_demand_hourly
        ops.vw_latest_dq_result
    """
    return spark.read.synapsesql(
        f"lh_gridpulse.{object_name}"
    )


# Trusted reusable sources for demand investigation
demand_source_df = read_gridpulse_sql_object(
    "gold.fact_market_demand_hourly"
)

latest_dq_source_df = read_gridpulse_sql_object(
    "ops.vw_latest_dq_result"
)


print("Demand source rows:", demand_source_df.count())
print("Latest DQ rows:", latest_dq_source_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Investigation Adapter: Demand
# Direct governed SQL object implementation
# ============================================================

from datetime import datetime


def _validate_market_date(value: str) -> str:
    """Validate YYYY-MM-DD and return canonical form."""
    parsed = datetime.strptime(value, "%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d")


def _validate_hour_ending(value):
    """GridPulse Hour Ending domain: 1..24."""
    if value is None:
        return None

    if not isinstance(value, int) or not 1 <= value <= 24:
        raise ValueError(
            "hour_ending must be an integer from 1 to 24"
        )

    return value


def investigate_demand(market_date: str, hour_ending=None):
    """
    Evidence-aware demand investigation.

    Trusted evidence:
      - gold.fact_market_demand_hourly
      - ops.vw_latest_dq_result

    hour_ending=None:
      full-day scope

    hour_ending=<1..24>:
      exact-hour scope
    """

    market_date = _validate_market_date(market_date)
    hour_ending = _validate_hour_ending(hour_ending)

    requested_date = F.to_date(F.lit(market_date))

    # --------------------------------------------------------
    # 1. Trusted demand evidence for requested date
    # --------------------------------------------------------

    date_df = (
        demand_source_df
        .filter(F.col("market_date") == requested_date)
    )

    if hour_ending is None:
        result_df = date_df
    else:
        result_df = (
            date_df
            .filter(F.col("hour_ending") == hour_ending)
        )

    result_df = result_df.orderBy("hour_ending")

    demand_rows = [
        row.asDict()
        for row in result_df.collect()
    ]

    # --------------------------------------------------------
    # 2. Neutral date-level coverage evidence
    # --------------------------------------------------------

    coverage_row = (
        date_df
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct("hour_ending")
                .alias("distinct_hour_count"),
            F.min("hour_ending").alias("min_hour"),
            F.max("hour_ending").alias("max_hour"),
            F.sum(
                F.when(
                    F.col("ontario_demand_mw").isNull()
                    | F.col("market_demand_mw").isNull(),
                    1,
                ).otherwise(0)
            ).alias("null_metric_rows"),
        )
        .first()
    )

    coverage = coverage_row.asDict()

    # Spark SUM on an empty dataset may return None.
    if coverage["null_metric_rows"] is None:
        coverage["null_metric_rows"] = 0

    # --------------------------------------------------------
    # 3. Applicable latest Batch DQ evidence
    # --------------------------------------------------------

    dq_df = (
        latest_dq_source_df
        .filter(
            F.col("dataset_name")
            == "gold.fact_market_demand_hourly"
        )
        .select(
            "rule_id",
            "rule_category",
            F.col("severity").alias("rule_outcome"),
            F.col("status").alias("execution_status"),
            "records_checked",
            "records_failed",
            F.col("execution_timestamp")
                .alias("evaluated_at"),
        )
        .orderBy("rule_id")
    )

    dq_rows = [
        row.asDict()
        for row in dq_df.collect()
    ]

    # --------------------------------------------------------
    # 4. Deterministic evidence facts
    # --------------------------------------------------------

    primary_evidence_exists = len(demand_rows) > 0

    required_metrics_usable = (
        primary_evidence_exists
        and all(
            row["ontario_demand_mw"] is not None
            and row["market_demand_mw"] is not None
            for row in demand_rows
        )
    )

    if hour_ending is None:

        # Full-day contract expects HE1..HE24.
        requested_scope_complete = (
            coverage["distinct_hour_count"] == 24
            and coverage["min_hour"] == 1
            and coverage["max_hour"] == 24
        )

    else:

        # Exact-hour request requires exactly one observation.
        requested_scope_complete = (
            len(demand_rows) == 1
            and demand_rows[0]["hour_ending"] == hour_ending
        )

    blocking_dq_failure = any(
        row["rule_outcome"] == "FAIL"
        for row in dq_rows
    )

    # --------------------------------------------------------
    # 5. Deterministic evidence-state classification
    # --------------------------------------------------------

    evidence = derive_evidence_state(
        primary_evidence_exists=primary_evidence_exists,
        required_metrics_usable=required_metrics_usable,
        requested_scope_complete=requested_scope_complete,
        blocking_dq_failure=blocking_dq_failure,
    )

    # --------------------------------------------------------
    # 6. Structured response
    # --------------------------------------------------------

    return {
        "tool_name": "get_demand",
        "contract_version": "1.0",

        "request": {
            "market_date": market_date,
            "hour_ending": hour_ending,
            "scope": (
                "FULL_DAY"
                if hour_ending is None
                else "EXACT_HOUR"
            ),
        },

        "execution": {
            "status": "SUCCESS",
        },

        "evidence": {
            **evidence,
            "coverage": coverage,
            "dq_rule_count": len(dq_rows),
            "blocking_dq_failure": blocking_dq_failure,
            "sources": [
                "gold.fact_market_demand_hourly",
                "ops.vw_latest_dq_result",
            ],
        },

        "result": demand_rows,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Real Demand Investigation Integration Tests
# ============================================================

demand_test_cases = [
    {
        "name": "Complete day",
        "date": "2026-08-28",
        "hour": None,
        "expected": "SUFFICIENT",
        "expected_rows": 24,
    },
    {
        "name": "Partial latest day",
        "date": "2026-08-29",
        "hour": None,
        "expected": "PARTIAL",
        "expected_rows": 1,
    },
    {
        "name": "Exact available hour",
        "date": "2026-08-29",
        "hour": 1,
        "expected": "SUFFICIENT",
        "expected_rows": 1,
    },
    {
        "name": "Missing historical date",
        "date": "1900-01-01",
        "hour": 18,
        "expected": "INSUFFICIENT",
        "expected_rows": 0,
    },
]


integration_results = []

for case in demand_test_cases:

    response = investigate_demand(
        case["date"],
        case["hour"],
    )

    actual_state = response["evidence"]["evidence_state"]
    actual_rows = len(response["result"])

    state_passed = actual_state == case["expected"]
    rows_passed = actual_rows == case["expected_rows"]

    passed = state_passed and rows_passed

    integration_results.append(
        {
            "test": case["name"],
            "expected_state": case["expected"],
            "actual_state": actual_state,
            "expected_rows": case["expected_rows"],
            "actual_rows": actual_rows,
            "reason_codes": ", ".join(
                response["evidence"]["reason_codes"]
            ),
            "passed": passed,
        }
    )

    assert passed, (
        f"{case['name']} failed | "
        f"expected_state={case['expected']} "
        f"actual_state={actual_state} | "
        f"expected_rows={case['expected_rows']} "
        f"actual_rows={actual_rows}"
    )


display(
    spark.createDataFrame(integration_results)
    .select(
        "test",
        "expected_state",
        "actual_state",
        "expected_rows",
        "actual_rows",
        "reason_codes",
        "passed",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Governed Eventhouse KQL Reader
# ============================================================

from notebookutils import mssparkutils
from pyspark.sql import functions as F

KUSTO_URI = (
    "https://trd-bwm1mgx3fpsmq71u2j.z3.kusto.fabric.microsoft.com"
)

KUSTO_DATABASE = "eh_gridpulse_realtime"

kusto_access_token = (
    mssparkutils.credentials.getToken("kusto")
)


def run_gridpulse_kql(query: str):
    """
    Execute controlled KQL against the governed
    GridPulse Eventhouse database.
    """
    return (
        spark.read
        .format("com.microsoft.kusto.spark.synapse.datasource")
        .option("accessToken", kusto_access_token)
        .option("kustoCluster", KUSTO_URI)
        .option("kustoDatabase", KUSTO_DATABASE)
        .option("kustoQuery", query)
        .load()
    )


# ------------------------------------------------------------
# Trusted Real-Time serving sources
# ------------------------------------------------------------

rt_current_source_df = run_gridpulse_kql("""
fn_rt_price_current()
| project
    delivery_date,
    delivery_hour,
    interval,
    zonal_price_capped_cad_per_mwh,
    loss_price_capped_cad_per_mwh,
    congestion_price_capped_cad_per_mwh,
    source_flag,
    publication_state,
    event_id,
    source_hash,
    event_created_at_utc
""")


rt_dq_source_df = run_gridpulse_kql("""
fn_rt_dq_summary()
| project
    rule_id,
    rule_name,
    severity,
    status,
    metric_value,
    evaluated_at_utc
""")


print("Current RT rows:", rt_current_source_df.count())
print("Current RT DQ rules:", rt_dq_source_df.count())

display(rt_dq_source_df.orderBy("rule_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Investigation Adapter: Current Real-Time Price
# ============================================================


def _validate_rt_interval(value):
    """
    GridPulse Real-Time 5-minute interval domain:
    intervals 1..12 per delivery hour.
    """
    if not isinstance(value, int) or not 1 <= value <= 12:
        raise ValueError(
            "interval must be an integer from 1 to 12"
        )

    return value


def investigate_realtime_price(
    delivery_date: str,
    delivery_hour: int,
    interval: int,
):
    """
    Evidence-aware current Real-Time price investigation.

    Trusted evidence:
      - fn_rt_price_current()
      - fn_rt_dq_summary()
    """

    delivery_date = _validate_market_date(delivery_date)
    delivery_hour = _validate_hour_ending(delivery_hour)
    interval = _validate_rt_interval(interval)

    # --------------------------------------------------------
    # 1. Exact current logical observation
    # --------------------------------------------------------

    result_df = (
        rt_current_source_df
        .filter(F.col("delivery_date") == delivery_date)
        .filter(F.col("delivery_hour") == delivery_hour)
        .filter(F.col("interval") == interval)
    )

    rt_rows = [
        row.asDict()
        for row in result_df.collect()
    ]

    # --------------------------------------------------------
    # 2. Normalize Real-Time DQ semantics
    #
    # RT:
    #   severity = rule criticality
    #   status   = rule outcome
    # --------------------------------------------------------

    dq_rows = []

    for row in rt_dq_source_df.collect():

        raw = row.asDict()

        dq_rows.append(
            {
                "rule_id": raw["rule_id"],
                "rule_name": raw["rule_name"],
                "rule_criticality": raw["severity"],
                "rule_outcome": raw["status"],
                "metric_value": raw["metric_value"],
                "evaluated_at": raw["evaluated_at_utc"],
            }
        )

    # --------------------------------------------------------
    # 3. Deterministic evidence facts
    # --------------------------------------------------------

    primary_evidence_exists = len(rt_rows) > 0

    required_metrics_usable = (
        primary_evidence_exists
        and all(
            row["zonal_price_capped_cad_per_mwh"] is not None
            and row["loss_price_capped_cad_per_mwh"] is not None
            and row[
                "congestion_price_capped_cad_per_mwh"
            ] is not None
            for row in rt_rows
        )
    )

    # Current logical serving state must contain at most
    # one row for one explicit business key.
    requested_scope_complete = len(rt_rows) == 1

    # In RT semantics, a blocking failure requires BOTH:
    #   rule outcome      = FAIL
    #   rule criticality  = FAIL
    blocking_dq_failure = any(
        row["rule_outcome"] == "FAIL"
        and row["rule_criticality"] == "FAIL"
        for row in dq_rows
    )

    nonblocking_dq_warning = any(
        row["rule_outcome"] == "FAIL"
        and row["rule_criticality"] == "WARN"
        for row in dq_rows
    )

    # --------------------------------------------------------
    # 4. Evidence state
    # --------------------------------------------------------

    evidence = derive_evidence_state(
        primary_evidence_exists=primary_evidence_exists,
        required_metrics_usable=required_metrics_usable,
        requested_scope_complete=requested_scope_complete,
        blocking_dq_failure=blocking_dq_failure,
    )

    # --------------------------------------------------------
    # 5. Structured response
    # --------------------------------------------------------

    return {
        "tool_name": "get_realtime_price",
        "contract_version": "1.0",

        "request": {
            "delivery_date": delivery_date,
            "delivery_hour": delivery_hour,
            "interval": interval,
        },

        "execution": {
            "status": "SUCCESS",
        },

        "evidence": {
            **evidence,

            "dq_rule_count": len(dq_rows),

            "blocking_dq_failure":
                blocking_dq_failure,

            "nonblocking_dq_warning":
                nonblocking_dq_warning,

            "sources": [
                "fn_rt_price_current()",
                "fn_rt_dq_summary()",
            ],
        },

        "result": rt_rows,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Current Real-Time Integration Tests
# ============================================================

latest_rt_row = (
    rt_current_source_df
    .orderBy(
        F.col("delivery_date").desc(),
        F.col("delivery_hour").desc(),
        F.col("interval").desc(),
    )
    .first()
)

assert latest_rt_row is not None, (
    "No current Real-Time evidence is available."
)

sample_date = latest_rt_row["delivery_date"]
sample_hour = latest_rt_row["delivery_hour"]
sample_interval = latest_rt_row["interval"]


rt_test_cases = [
    {
        "name": "Existing current RT observation",
        "date": sample_date,
        "hour": sample_hour,
        "interval": sample_interval,
        "expected": "SUFFICIENT",
        "expected_rows": 1,
    },
    {
        "name": "Missing RT observation",
        "date": "1900-01-01",
        "hour": 18,
        "interval": 7,
        "expected": "INSUFFICIENT",
        "expected_rows": 0,
    },
]


rt_integration_results = []

for case in rt_test_cases:

    response = investigate_realtime_price(
        case["date"],
        case["hour"],
        case["interval"],
    )

    actual_state = response["evidence"]["evidence_state"]
    actual_rows = len(response["result"])

    passed = (
        actual_state == case["expected"]
        and actual_rows == case["expected_rows"]
    )

    rt_integration_results.append(
        {
            "test": case["name"],
            "delivery_date": case["date"],
            "delivery_hour": case["hour"],
            "interval": case["interval"],
            "expected_state": case["expected"],
            "actual_state": actual_state,
            "expected_rows": case["expected_rows"],
            "actual_rows": actual_rows,
            "blocking_dq_failure":
                response["evidence"][
                    "blocking_dq_failure"
                ],
            "passed": passed,
        }
    )

    assert passed, (
        f"{case['name']} failed | "
        f"expected_state={case['expected']} "
        f"actual_state={actual_state} | "
        f"expected_rows={case['expected_rows']} "
        f"actual_rows={actual_rows}"
    )


display(
    spark.createDataFrame(rt_integration_results)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Governed Real-Time Market Metrics Source
# ============================================================

rt_market_metrics_df = run_gridpulse_kql("""
fn_rt_price_market_metrics()
| project
    delivery_date,
    delivery_hour,
    interval_no,
    price,
    previous_price,
    slot_gap,
    interval_price_change,
    is_negative_price,

    rolling_15_complete,
    rolling_15_avg,
    rolling_15_min,
    rolling_15_max,
    rolling_15_stddev,

    rolling_30_complete,
    rolling_30_avg,
    rolling_30_min,
    rolling_30_max,
    rolling_30_stddev,

    rolling_60_complete,
    rolling_60_avg,
    rolling_60_min,
    rolling_60_max,
    rolling_60_stddev,

    event_id,
    source_hash,
    event_created_at_utc
""")

print(
    "RT market-metrics rows:",
    rt_market_metrics_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Composite Investigation: investigate_market_event
# ============================================================

def investigate_market_event(
    delivery_date: str,
    delivery_hour: int,
    interval: int,
):
    """
    Composite evidence-aware market investigation.

    Core event:
      current Real-Time price + deterministic RT market metrics.

    Context:
      hourly Ontario / market demand when available.

    Important:
      This function describes concurrent evidence.
      It does NOT establish causality.
    """

    delivery_date = _validate_market_date(delivery_date)
    delivery_hour = _validate_hour_ending(delivery_hour)
    interval = _validate_rt_interval(interval)

    # --------------------------------------------------------
    # 1. Core current RT evidence
    # --------------------------------------------------------

    rt_response = investigate_realtime_price(
        delivery_date,
        delivery_hour,
        interval,
    )

    rt_state = rt_response["evidence"]["evidence_state"]

    # --------------------------------------------------------
    # 2. Deterministic RT market metrics
    # --------------------------------------------------------

    metrics_df = (
        rt_market_metrics_df
        .filter(F.col("delivery_date") == delivery_date)
        .filter(F.col("delivery_hour") == delivery_hour)
        .filter(F.col("interval_no") == interval)
    )

    metrics_rows = [
        row.asDict()
        for row in metrics_df.collect()
    ]

    metrics_available = len(metrics_rows) == 1

    # --------------------------------------------------------
    # 3. Hourly Batch demand context
    #
    # Mapping is business-key alignment only:
    # delivery_date -> market_date
    # delivery_hour -> hour_ending
    #
    # No timezone conversion is inferred.
    # --------------------------------------------------------

    demand_response = investigate_demand(
        delivery_date,
        delivery_hour,
    )

    demand_state = (
        demand_response["evidence"]["evidence_state"]
    )

    # --------------------------------------------------------
    # 4. Composite evidence state
    #
    # Core RT event is mandatory.
    # Demand is contextual.
    # --------------------------------------------------------

    if rt_state == "BLOCKED_BY_DQ":
        overall_state = "BLOCKED_BY_DQ"
        overall_reasons = ["CORE_RT_BLOCKED_BY_DQ"]

    elif rt_state == "INSUFFICIENT":
        overall_state = "INSUFFICIENT"
        overall_reasons = ["CORE_RT_INSUFFICIENT"]

    elif not metrics_available:
        overall_state = "PARTIAL"
        overall_reasons = [
            "RT_METRICS_UNAVAILABLE"
        ]

    elif demand_state != "SUFFICIENT":
        overall_state = "PARTIAL"
        overall_reasons = [
            "HOURLY_DEMAND_CONTEXT_UNAVAILABLE"
        ]

    else:
        overall_state = "SUFFICIENT"
        overall_reasons = ["EVIDENCE_COMPLETE"]

    # --------------------------------------------------------
    # 5. Guardrail notes
    # --------------------------------------------------------

    limitations = [
        (
            "Hourly demand context is coarser than the "
            "5-minute Real-Time interval."
        ),
        (
            "Concurrent market conditions do not establish "
            "causality for the Real-Time price movement."
        ),
    ]

    # --------------------------------------------------------
    # 6. Structured composite result
    # --------------------------------------------------------

    return {
        "tool_name": "investigate_market_event",
        "contract_version": "1.0",

        "request": {
            "delivery_date": delivery_date,
            "delivery_hour": delivery_hour,
            "interval": interval,
        },

        "execution": {
            "status": "SUCCESS",
        },

        "evidence": {
            "evidence_state": overall_state,
            "reason_codes": overall_reasons,

            "component_states": {
                "realtime_price": rt_state,
                "realtime_metrics": (
                    "SUFFICIENT"
                    if metrics_available
                    else "INSUFFICIENT"
                ),
                "hourly_demand_context": demand_state,
            },

            "sources": [
                "fn_rt_price_current()",
                "fn_rt_price_market_metrics()",
                "fn_rt_dq_summary()",
                "gold.fact_market_demand_hourly",
                "ops.vw_latest_dq_result",
            ],
        },

        "result": {
            "realtime_price": rt_response["result"],
            "realtime_metrics": metrics_rows,
            "hourly_demand_context":
                demand_response["result"],
        },

        "limitations": limitations,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Composite Investigation Integration Tests
# ============================================================

sample_rt = (
    rt_current_source_df
    .orderBy(
        F.col("delivery_date").desc(),
        F.col("delivery_hour").desc(),
        F.col("interval").desc(),
    )
    .first()
)

assert sample_rt is not None


composite_test_cases = [
    {
        "name": "Latest real RT market event",
        "date": sample_rt["delivery_date"],
        "hour": sample_rt["delivery_hour"],
        "interval": sample_rt["interval"],
        "expected": "PARTIAL",
    },
    {
        "name": "Missing market event",
        "date": "1900-01-01",
        "hour": 18,
        "interval": 7,
        "expected": "INSUFFICIENT",
    },
]


composite_results = []

for case in composite_test_cases:

    response = investigate_market_event(
        case["date"],
        case["hour"],
        case["interval"],
    )

    actual = response["evidence"]["evidence_state"]

    passed = actual == case["expected"]

    component_states = (
        response["evidence"]["component_states"]
    )

    composite_results.append(
        {
            "test": case["name"],
            "delivery_date": case["date"],
            "delivery_hour": case["hour"],
            "interval": case["interval"],
            "expected_state": case["expected"],
            "actual_state": actual,
            "rt_state":
                component_states["realtime_price"],
            "rt_metrics_state":
                component_states["realtime_metrics"],
            "demand_state":
                component_states[
                    "hourly_demand_context"
                ],
            "reason_codes": ", ".join(
                response["evidence"]["reason_codes"]
            ),
            "passed": passed,
        }
    )

    assert passed, (
        f"{case['name']} failed | "
        f"expected={case['expected']} "
        f"actual={actual}"
    )


display(
    spark.createDataFrame(composite_results)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# GRIDPULSE AI — PHASE 5
# Final Investigation Regression Suite
# ============================================================

final_evaluation_results = []


def record_test(name, expected, actual, passed, category):
    final_evaluation_results.append(
        {
            "category": category,
            "test": name,
            "expected": str(expected),
            "actual": str(actual),
            "passed": bool(passed),
        }
    )


# ------------------------------------------------------------
# 1. Complete Batch evidence
# ------------------------------------------------------------

response = investigate_demand(
    "2026-08-28",
    None,
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Complete demand day",
    "SUFFICIENT",
    actual,
    actual == "SUFFICIENT",
    "EVIDENCE",
)


# ------------------------------------------------------------
# 2. Partial Batch coverage
# ------------------------------------------------------------

response = investigate_demand(
    "2026-08-29",
    None,
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Partial demand day",
    "PARTIAL",
    actual,
    actual == "PARTIAL",
    "EVIDENCE",
)


# ------------------------------------------------------------
# 3. Missing Batch evidence
# ------------------------------------------------------------

response = investigate_demand(
    "1900-01-01",
    18,
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Missing demand observation",
    "INSUFFICIENT",
    actual,
    actual == "INSUFFICIENT",
    "EVIDENCE",
)


# ------------------------------------------------------------
# 4. Existing current RT evidence
# ------------------------------------------------------------

latest_rt = (
    rt_current_source_df
    .orderBy(
        F.col("delivery_date").desc(),
        F.col("delivery_hour").desc(),
        F.col("interval").desc(),
    )
    .first()
)

response = investigate_realtime_price(
    latest_rt["delivery_date"],
    latest_rt["delivery_hour"],
    latest_rt["interval"],
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Existing current RT observation",
    "SUFFICIENT",
    actual,
    actual == "SUFFICIENT",
    "EVIDENCE",
)


# ------------------------------------------------------------
# 5. Missing RT evidence
# ------------------------------------------------------------

response = investigate_realtime_price(
    "1900-01-01",
    18,
    7,
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Missing RT observation",
    "INSUFFICIENT",
    actual,
    actual == "INSUFFICIENT",
    "EVIDENCE",
)


# ------------------------------------------------------------
# 6. Composite graceful degradation
# ------------------------------------------------------------

response = investigate_market_event(
    latest_rt["delivery_date"],
    latest_rt["delivery_hour"],
    latest_rt["interval"],
)

actual = response["evidence"]["evidence_state"]

record_test(
    "Latest composite investigation",
    "PARTIAL",
    actual,
    actual == "PARTIAL",
    "COMPOSITE",
)


# ------------------------------------------------------------
# 7. Blocking-DQ policy
# Synthetic policy test only.
# Does NOT alter production DQ data.
# ------------------------------------------------------------

response = derive_evidence_state(
    primary_evidence_exists=True,
    required_metrics_usable=True,
    requested_scope_complete=True,
    blocking_dq_failure=True,
)

actual = response["evidence_state"]

record_test(
    "Blocking DQ policy",
    "BLOCKED_BY_DQ",
    actual,
    actual == "BLOCKED_BY_DQ",
    "DQ_POLICY",
)


# ------------------------------------------------------------
# 8. Invalid Hour Ending
# ------------------------------------------------------------

try:
    investigate_demand(
        "2026-08-28",
        25,
    )

    actual = "NO_ERROR"
    passed = False

except ValueError:
    actual = "ValueError"
    passed = True

record_test(
    "Reject invalid Hour Ending",
    "ValueError",
    actual,
    passed,
    "VALIDATION",
)


# ------------------------------------------------------------
# 9. Invalid RT interval
# ------------------------------------------------------------

try:
    investigate_realtime_price(
        "2026-09-04",
        18,
        13,
    )

    actual = "NO_ERROR"
    passed = False

except ValueError:
    actual = "ValueError"
    passed = True

record_test(
    "Reject invalid RT interval",
    "ValueError",
    actual,
    passed,
    "VALIDATION",
)


# ------------------------------------------------------------
# 10. Invalid date format
# ------------------------------------------------------------

try:
    investigate_demand(
        "08/28/2026",
        18,
    )

    actual = "NO_ERROR"
    passed = False

except ValueError:
    actual = "ValueError"
    passed = True

record_test(
    "Reject invalid date format",
    "ValueError",
    actual,
    passed,
    "VALIDATION",
)


# ------------------------------------------------------------
# Final regression gate
# ------------------------------------------------------------

evaluation_df = spark.createDataFrame(
    final_evaluation_results
)

display(
    evaluation_df
    .orderBy("category", "test")
)

passed_count = sum(
    1
    for row in final_evaluation_results
    if row["passed"]
)

total_count = len(final_evaluation_results)

print(
    f"FINAL PHASE 5 EVALUATION: "
    f"{passed_count}/{total_count} PASSED"
)

assert passed_count == total_count, (
    f"Phase 5 regression failed: "
    f"{passed_count}/{total_count} tests passed"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Phase 5 — Final Validation
# 
# GridPulse Phase 5 implements a governed, evidence-aware investigation layer across Microsoft Fabric Lakehouse and Eventhouse.
# 
# ## Implemented capabilities
# 
# ### Batch / SQL
# - Governed demand investigation
# - Peak-demand serving contract
# - Zonal-demand serving contract
# - Generation-mix serving contract
# - Day-Ahead price serving contract
# - Day-Ahead vs historical Real-Time comparison contract
# - Latest persisted Batch DQ state
# 
# ### Real-Time / KQL
# - Current logical Real-Time price state
# - Revision-aware logical event history
# - Gap-aware price-change metrics
# - Rolling 15/30/60-minute market context
# - Real-Time DQ state
# 
# ## Evidence model
# 
# Investigation results are classified deterministically as:
# 
# - `SUFFICIENT`
# - `PARTIAL`
# - `INSUFFICIENT`
# - `BLOCKED_BY_DQ`
# 
# Evidence state considers:
# 
# 1. Primary evidence availability
# 2. Required metric usability
# 3. Requested-scope completeness
# 4. Applicable data-quality state
# 
# `DQ PASS` does not imply that requested evidence exists, and missing data is never silently converted to zero.
# 
# ## Grounding guardrails
# 
# - Critical KPIs are calculated deterministically.
# - Negative electricity-price components are preserved as valid observations.
# - NULL values are not converted to zero.
# - Real-Time revision semantics are preserved.
# - Incomplete rolling windows are explicitly identified.
# - Hourly and 5-minute grains are not presented as equivalent.
# - Concurrent conditions are not presented as causal explanations.
# - The reasoning layer is not given arbitrary SQL or KQL execution capability.
# 
# ## Runtime boundary
# 
# The investigation layer is implemented natively in Microsoft Fabric using:
# 
# - Lakehouse SQL Analytics Endpoint
# - Eventhouse / KQL
# - Fabric Notebook orchestration
# - Persisted Batch and Real-Time DQ evidence
# 
# A Fabric Data Agent was evaluated but is not implemented in the current environment because the available Fabric Trial capacity does not support creation of the required Data Agent workload.
# 
# The investigation contracts remain runtime-independent and can later be exposed through a supported Fabric Data Agent or another governed agent runtime without changing the underlying deterministic data contracts.
# 
# ## Final evaluation
# 
# **Phase 5 regression suite: 10 / 10 tests passed.**
# 
# Validated behaviors include:
# 
# - Complete evidence
# - Partial coverage
# - Missing evidence
# - Current Real-Time evidence
# - Missing Real-Time evidence
# - Composite graceful degradation
# - Blocking DQ policy
# - Invalid Hour Ending rejection
# - Invalid Real-Time interval rejection
# - Invalid date-format rejection


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
