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

# # GridPulse AI — Source Discovery
# 
# **Notebook:** `nb_01_source_discovery`
# 
# ## Purpose
# 
# Profile and validate official IESO source datasets before defining production ingestion logic and data contracts.
# 
# ## Scope
# 
# - Source discovery
# - File inspection
# - Schema profiling
# - Grain validation
# - Null analysis
# - Duplicate analysis
# - Date coverage
# - Source quirks
# - Data contract evidence
# 
# ## Engineering principle
# 
# No source schema, grain, business rule, or ingestion assumption is considered final until validated against the actual source data.

# CELL ********************

from datetime import datetime, timezone

print("GridPulse AI - Source Discovery")
print(f"Execution time (UTC): {datetime.now(timezone.utc).isoformat()}")
print(f"Spark version: {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_root = "Files/bronze/ieso"

entries = notebookutils.fs.ls(bronze_root)

for entry in entries:
    print(entry.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EXPECTED_BRONZE_FOLDERS = {
    "demand",
    "demand_zonal",
    "generation",
    "price_day_ahead",
    "price_realtime",
}

entries = notebookutils.fs.ls("Files/bronze/ieso")

actual_folders = {
    entry.name.rstrip("/")
    for entry in entries
    if entry.isDir
}

missing_folders = EXPECTED_BRONZE_FOLDERS - actual_folders
unexpected_folders = actual_folders - EXPECTED_BRONZE_FOLDERS

print(f"Expected folders   : {sorted(EXPECTED_BRONZE_FOLDERS)}")
print(f"Actual folders     : {sorted(actual_folders)}")
print(f"Missing folders    : {sorted(missing_folders)}")
print(f"Unexpected folders : {sorted(unexpected_folders)}")

assert not missing_folders, (
    f"Bronze structure validation failed. "
    f"Missing folders: {sorted(missing_folders)}"
)

print("\nBronze landing zone validation: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source 01 — IESO Hourly Demand
# 
# **Business purpose:** Provincial hourly electricity demand analysis.
# 
# **Official source:** IESO Public Reports — Hourly Demand Report
# 
# **Discovery objectives:**
# - Preserve the raw source payload
# - Inspect source metadata
# - Discover the actual CSV structure
# - Profile schema and data coverage
# - Validate the candidate grain
# - Analyze nulls and duplicates
# - Identify source-specific quirks
# - Gather evidence for the future data contract
# 
# ### Adding Source 01 to NB
# ### Setting and downloading real source

# CELL ********************

from datetime import datetime, timezone
from hashlib import sha256
from urllib.request import Request, urlopen

SOURCE_CONFIG = {
    "source_name": "ieso_hourly_demand",
    "source_directory_url": "https://reports-public.ieso.ca/public/Demand/",
    "source_url": "https://reports-public.ieso.ca/public/Demand/PUB_Demand_2026.csv",
    "source_file_name": "PUB_Demand_2026.csv",
    "market_year": 2026,
    "bronze_directory": "Files/bronze/ieso/demand",
}

request = Request(
    SOURCE_CONFIG["source_url"],
    headers={
        "User-Agent": "GridPulseAI/1.0 SourceDiscovery"
    },
)

with urlopen(request, timeout=60) as response:
    http_status = response.status
    content_type = response.headers.get("Content-Type")
    raw_bytes = response.read()

assert http_status == 200, f"Unexpected HTTP status: {http_status}"
assert len(raw_bytes) > 0, "Source file is empty."

source_hash = sha256(raw_bytes).hexdigest()
raw_text = raw_bytes.decode("utf-8")

print(f"Source name       : {SOURCE_CONFIG['source_name']}")
print(f"HTTP status       : {http_status}")
print(f"Content type      : {content_type}")
print(f"File size (bytes) : {len(raw_bytes):,}")
print(f"SHA-256           : {source_hash}")
print(f"Retrieved at UTC  : {datetime.now(timezone.utc).isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Preserve Bronze sample immutable

# CELL ********************

hash_short = source_hash[:12]

bronze_file_name = (
    f"PUB_Demand_2026__sha256_{hash_short}.csv"
)

bronze_path = (
    f"{SOURCE_CONFIG['bronze_directory']}/"
    f"{bronze_file_name}"
)

if notebookutils.fs.exists(bronze_path):
    print("Payload already exists in Bronze.")
    print(f"Path: {bronze_path}")
else:
    write_success = notebookutils.fs.put(
        bronze_path,
        raw_text,
        overwrite=False,
    )

    assert write_success, "Bronze write failed."

    print("New payload written to Bronze.")
    print(f"Path: {bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verifying Bronze did not alter payload

# CELL ********************

from hashlib import sha256
from pathlib import Path

bronze_local_path = Path(
    "/lakehouse/default"
) / bronze_path

assert bronze_local_path.exists(), (
    f"Bronze file not found: {bronze_local_path}"
)

persisted_size = bronze_local_path.stat().st_size

hasher = sha256()

with bronze_local_path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        hasher.update(chunk)

persisted_hash = hasher.hexdigest()

print(f"Source size       : {len(raw_bytes):,}")
print(f"Bronze size       : {persisted_size:,}")
print()
print(f"Source SHA-256    : {source_hash}")
print(f"Bronze SHA-256    : {persisted_hash}")

assert persisted_size == len(raw_bytes), (
    "Bronze file size differs from source file size."
)

assert persisted_hash == source_hash, (
    "Bronze payload differs from source payload."
)

print("\nRaw payload integrity validation: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Source Inspection before parsing

# CELL ********************

source_lines = raw_text.splitlines()

print("=== FIRST PHYSICAL LINES FROM SOURCE ===\n")

for line_number, line in enumerate(source_lines[:12], start=1):
    print(f"{line_number:02d}: {line}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### General Header detection

# CELL ********************

header_candidates = []

for index, line in enumerate(source_lines[:25]):
    columns = [value.strip() for value in line.split(",")]

    if (
        len(columns) >= 2
        and columns[0] == "Date"
        and columns[1] == "Hour"
    ):
        header_candidates.append(index)

print(f"Header candidates: {header_candidates}")

assert len(header_candidates) == 1, (
    "Expected exactly one tabular header candidate."
)

header_row_index = header_candidates[0]

print(f"Detected header row index: {header_row_index}")
print(f"Detected header content  : {source_lines[header_row_index]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parsing tables

# CELL ********************

from io import StringIO

import pandas as pd

demand_df = pd.read_csv(
    StringIO(raw_text),
    skiprows=header_row_index
)

print("=== DISCOVERED SOURCE STRUCTURE ===\n")

print("Columns:")
for column in demand_df.columns:
    print(f" - {column}")

print("\nInferred data types:")
print(demand_df.dtypes)

print(f"\nRow count: {len(demand_df):,}")

display(demand_df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Minimal Structural Validation

# CELL ********************

discovered_columns = demand_df.columns.tolist()

print(f"Discovered columns: {discovered_columns}")
print(f"Column count      : {len(discovered_columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EXPECTED_DISCOVERED_COLUMNS = [
    "Date",
    "Hour",
    "Market Demand",
    "Ontario Demand",
]

assert discovered_columns == EXPECTED_DISCOVERED_COLUMNS, (
    "Source schema differs from the structure observed during discovery. "
    f"Expected: {EXPECTED_DISCOVERED_COLUMNS}. "
    f"Actual: {discovered_columns}."
)

print("Source schema validation: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### General Profiling

# CELL ********************

profile_df = demand_df.copy()

profile_df["_parsed_date"] = pd.to_datetime(
    profile_df["Date"],
    errors="coerce"
)

print("=== HOURLY DEMAND SOURCE PROFILE ===\n")

print(f"Rows             : {len(profile_df):,}")
print(f"Source columns   : {len(demand_df.columns):,}")
print(f"Minimum date     : {profile_df['_parsed_date'].min()}")
print(f"Maximum date     : {profile_df['_parsed_date'].max()}")
print(f"Invalid dates    : {profile_df['_parsed_date'].isna().sum():,}")
print(f"Minimum hour     : {demand_df['Hour'].min()}")
print(f"Maximum hour     : {demand_df['Hour'].max()}")

print("\n=== NULL COUNTS ===")
print(demand_df.isna().sum())

print("\n=== DISTINCT COUNTS ===")
print(demand_df.nunique())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data types validation

# CELL ********************

numeric_checks = {}

for column in [
    "Hour",
    "Market Demand",
    "Ontario Demand",
]:
    converted = pd.to_numeric(
        demand_df[column],
        errors="coerce"
    )

    invalid_count = converted.isna().sum()

    numeric_checks[column] = invalid_count

    print(
        f"{column:<20} "
        f"non-numeric/null after conversion: "
        f"{invalid_count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert numeric_checks["Hour"] == 0, (
    "Hour contains values that cannot be interpreted numerically."
)

print("\nBasic numeric parsing validation: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Grain Candidates

# CELL ********************

GRAIN_COLUMNS = [
    "Date",
    "Hour",
]

unique_grain_count = (
    demand_df[GRAIN_COLUMNS]
    .drop_duplicates()
    .shape[0]
)

duplicate_mask = demand_df.duplicated(
    subset=GRAIN_COLUMNS,
    keep=False
)

duplicate_rows = demand_df.loc[duplicate_mask]

duplicate_keys = (
    duplicate_rows[GRAIN_COLUMNS]
    .drop_duplicates()
)

print("=== CANDIDATE GRAIN VALIDATION ===\n")

print(f"Candidate grain : {GRAIN_COLUMNS}")
print(f"Total rows      : {len(demand_df):,}")
print(f"Unique keys     : {unique_grain_count:,}")
print(f"Duplicate rows  : {len(duplicate_rows):,}")
print(f"Duplicate keys  : {len(duplicate_keys):,}")

if duplicate_keys.empty:
    print("\nCandidate grain validation: PASS")
else:
    print("\nCandidate grain validation: INVESTIGATE")
    display(duplicate_rows.sort_values(GRAIN_COLUMNS).head(25))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Observations per day

# CELL ********************

rows_per_day = (
    demand_df
    .groupby("Date")
    .size()
    .rename("row_count")
    .reset_index()
)

row_count_distribution = (
    rows_per_day["row_count"]
    .value_counts()
    .rename_axis("rows_per_day")
    .reset_index(name="number_of_days")
    .sort_values("rows_per_day")
)

print("=== DAILY ROW COUNT DISTRIBUTION ===")

display(row_count_distribution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

non_24_days = rows_per_day[
    rows_per_day["row_count"] != 24
].copy()

print(
    f"Days with row count different from 24: "
    f"{len(non_24_days):,}"
)

display(non_24_days.tail(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Hour Distribution

# CELL ********************

hour_distribution = (
    demand_df["Hour"]
    .value_counts(dropna=False)
    .rename_axis("hour")
    .reset_index(name="record_count")
    .sort_values("hour")
)

display(hour_distribution)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

observed_dates = pd.DatetimeIndex(
    profile_df["_parsed_date"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

expected_dates = pd.date_range(
    start=observed_dates.min(),
    end=observed_dates.max(),
    freq="D"
)

missing_dates = expected_dates.difference(observed_dates)

print("=== DATE CONTINUITY VALIDATION ===\n")

print(f"Minimum date        : {observed_dates.min().date()}")
print(f"Maximum date        : {observed_dates.max().date()}")
print(f"Observed dates      : {len(observed_dates):,}")
print(f"Expected dates      : {len(expected_dates):,}")
print(f"Missing dates       : {len(missing_dates):,}")

if len(missing_dates) > 0:
    print("\nMissing calendar dates:")
    for date in missing_dates:
        print(f" - {date.date()}")
else:
    print("\nDate continuity validation: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest_date = demand_df["Date"].max()

historical_daily_counts = rows_per_day[
    rows_per_day["Date"] != latest_date
].copy()

historical_incomplete_days = historical_daily_counts[
    historical_daily_counts["row_count"] != 24
]

latest_day_count = int(
    rows_per_day.loc[
        rows_per_day["Date"] == latest_date,
        "row_count"
    ].iloc[0]
)

print("=== COMPLETENESS ASSESSMENT ===\n")

print(f"Latest source date                  : {latest_date}")
print(f"Rows on latest source date          : {latest_day_count}")
print(
    "Historical dates with != 24 rows   : "
    f"{len(historical_incomplete_days)}"
)

if historical_incomplete_days.empty:
    print("\nHistorical completeness check: PASS")
else:
    print("\nHistorical completeness check: INVESTIGATE")
    display(historical_incomplete_days)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reusable Source Discovery Framework
# 
# This framework standardizes source inspection without imposing a common business schema across heterogeneous IESO reports.
# 
# ### Responsibilities
# 
# - Detect tabular CSV headers
# - Validate observed source columns
# - Parse source payloads
# - Profile data types
# - Profile nulls and distinct values
# - Validate candidate grains
# - Validate date continuity
# - Assess historical daily completeness
# - Profile numeric fields
# 
# Source-specific expectations remain configuration-driven.

# CELL ********************

import csv

from dataclasses import dataclass
from io import StringIO
from typing import Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class SourceDiscoveryConfig:
    source_name: str
    required_header_columns: Tuple[str, ...]
    expected_columns: Tuple[str, ...]
    candidate_grain: Tuple[str, ...]
    date_column: Optional[str] = None
    numeric_columns: Tuple[str, ...] = ()
    observed_rows_per_complete_day: Optional[int] = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def detect_csv_header(
    raw_text: str,
    required_columns: Tuple[str, ...],
    search_limit: int = 50,
) -> int:
    """
    Detect the physical row containing the tabular CSV header.

    The function searches for exactly one row containing all required columns.
    """

    source_lines = raw_text.splitlines()
    candidates = []

    for index, line in enumerate(source_lines[:search_limit]):
        try:
            parsed_row = next(csv.reader([line]))
        except csv.Error:
            continue

        normalized_values = tuple(
            value.strip()
            for value in parsed_row
        )

        if all(
            column in normalized_values
            for column in required_columns
        ):
            candidates.append(index)

    if len(candidates) != 1:
        raise ValueError(
            "Unable to uniquely identify the tabular header. "
            f"Candidates found: {candidates}"
        )

    return candidates[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_csv_source(
    raw_text: str,
    config: SourceDiscoveryConfig,
):
    """
    Detect and parse the tabular portion of a CSV source payload.
    """

    header_row_index = detect_csv_header(
        raw_text=raw_text,
        required_columns=config.required_header_columns,
    )

    dataframe = pd.read_csv(
        StringIO(raw_text),
        skiprows=header_row_index,
    )

    discovered_columns = tuple(dataframe.columns.tolist())

    if discovered_columns != config.expected_columns:
        raise ValueError(
            "Observed source schema differs from discovery expectation. "
            f"Expected: {config.expected_columns}. "
            f"Actual: {discovered_columns}."
        )

    return dataframe, header_row_index

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_source_profile(
    dataframe: pd.DataFrame,
    config: SourceDiscoveryConfig,
    header_row_index: int,
) -> dict:
    """
    Build a standardized discovery profile for a source dataframe.

    The profile reports evidence. It does not automatically assign
    business-level PASS/WARN/FAIL severity.
    """

    profile = {
        "source_name": config.source_name,
        "header_row_index": int(header_row_index),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": dataframe.columns.tolist(),
        "dtypes": {
            column: str(dtype)
            for column, dtype in dataframe.dtypes.items()
        },
        "null_counts": {
            column: int(value)
            for column, value in dataframe.isna().sum().items()
        },
        "distinct_counts": {
            column: int(value)
            for column, value in dataframe.nunique(
                dropna=True
            ).items()
        },
    }

    # Candidate grain profiling
    if config.candidate_grain:
        grain_columns = list(config.candidate_grain)

        duplicate_mask = dataframe.duplicated(
            subset=grain_columns,
            keep=False,
        )

        duplicate_rows = dataframe.loc[duplicate_mask]

        duplicate_key_count = (
            duplicate_rows[grain_columns]
            .drop_duplicates()
            .shape[0]
        )

        profile["candidate_grain"] = grain_columns
        profile["duplicate_row_count"] = int(
            len(duplicate_rows)
        )
        profile["duplicate_key_count"] = int(
            duplicate_key_count
        )

    # Numeric profiling
    numeric_profile = {}

    for column in config.numeric_columns:
        converted = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

        valid_values = converted.dropna()

        numeric_profile[column] = {
            "non_numeric_or_null_count": int(
                converted.isna().sum()
            ),
            "minimum": (
                valid_values.min()
                if not valid_values.empty
                else None
            ),
            "maximum": (
                valid_values.max()
                if not valid_values.empty
                else None
            ),
        }

    profile["numeric_profile"] = numeric_profile

    # Date profiling
    if config.date_column:
        parsed_dates = pd.to_datetime(
            dataframe[config.date_column],
            errors="coerce",
        )

        profile["invalid_date_count"] = int(
            parsed_dates.isna().sum()
        )

        valid_dates = parsed_dates.dropna()

        if not valid_dates.empty:
            normalized_dates = valid_dates.dt.normalize()

            minimum_date = normalized_dates.min()
            maximum_date = normalized_dates.max()

            profile["minimum_date"] = (
                minimum_date.date().isoformat()
            )

            profile["maximum_date"] = (
                maximum_date.date().isoformat()
            )

            observed_dates = pd.DatetimeIndex(
                normalized_dates
                .drop_duplicates()
                .sort_values()
            )

            expected_dates = pd.date_range(
                start=minimum_date,
                end=maximum_date,
                freq="D",
            )

            missing_dates = expected_dates.difference(
                observed_dates
            )

            profile["observed_date_count"] = int(
                len(observed_dates)
            )

            profile["missing_date_count"] = int(
                len(missing_dates)
            )

            profile["missing_dates"] = [
                date.date().isoformat()
                for date in missing_dates
            ]

            daily_counts = (
                pd.DataFrame(
                    {
                        "date": normalized_dates
                    }
                )
                .groupby("date")
                .size()
            )

            distribution = (
                daily_counts
                .value_counts()
                .sort_index()
            )

            profile["daily_row_count_distribution"] = {
                int(row_count): int(number_of_days)
                for row_count, number_of_days
                in distribution.items()
            }

            latest_date = daily_counts.index.max()

            profile["latest_date_row_count"] = int(
                daily_counts.loc[latest_date]
            )

            if (
                config.observed_rows_per_complete_day
                is not None
            ):
                historical_counts = daily_counts[
                    daily_counts.index != latest_date
                ]

                historical_incomplete = (
                    historical_counts[
                        historical_counts
                        != config.observed_rows_per_complete_day
                    ]
                )

                profile[
                    "historical_incomplete_day_count"
                ] = int(
                    len(historical_incomplete)
                )

                profile[
                    "historical_incomplete_dates"
                ] = {
                    date.date().isoformat(): int(count)
                    for date, count
                    in historical_incomplete.items()
                }

    return profile

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def print_source_profile(profile: dict) -> None:
    """
    Print a concise human-readable discovery summary.
    """

    print(
        f"=== SOURCE PROFILE: "
        f"{profile['source_name']} ===\n"
    )

    print(
        f"Header row index       : "
        f"{profile['header_row_index']}"
    )

    print(
        f"Rows                   : "
        f"{profile['row_count']:,}"
    )

    print(
        f"Columns                : "
        f"{profile['column_count']}"
    )

    print(
        f"Candidate grain        : "
        f"{profile.get('candidate_grain')}"
    )

    print(
        f"Duplicate grain keys   : "
        f"{profile.get('duplicate_key_count')}"
    )

    if "minimum_date" in profile:
        print(
            f"Date range              : "
            f"{profile['minimum_date']} "
            f"→ {profile['maximum_date']}"
        )

        print(
            f"Missing calendar dates : "
            f"{profile['missing_date_count']}"
        )

        print(
            f"Latest date row count  : "
            f"{profile['latest_date_row_count']}"
        )

    if "historical_incomplete_day_count" in profile:
        print(
            f"Historical incomplete : "
            f"{profile['historical_incomplete_day_count']}"
        )

    print("\nNull counts:")

    for column, count in profile["null_counts"].items():
        print(
            f"  {column:<25}: {count:,}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

HOURLY_DEMAND_CONFIG = SourceDiscoveryConfig(
    source_name="ieso_hourly_demand",

    required_header_columns=(
        "Date",
        "Hour",
    ),

    expected_columns=(
        "Date",
        "Hour",
        "Market Demand",
        "Ontario Demand",
    ),

    candidate_grain=(
        "Date",
        "Hour",
    ),

    date_column="Date",

    numeric_columns=(
        "Hour",
        "Market Demand",
        "Ontario Demand",
    ),

    observed_rows_per_complete_day=24,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_framework_df, demand_header_index = (
    parse_csv_source(
        raw_text=raw_text,
        config=HOURLY_DEMAND_CONFIG,
    )
)

demand_profile = build_source_profile(
    dataframe=demand_framework_df,
    config=HOURLY_DEMAND_CONFIG,
    header_row_index=demand_header_index,
)

print_source_profile(demand_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert demand_header_index == header_row_index

assert len(demand_framework_df) == len(demand_df)

assert (
    demand_framework_df.columns.tolist()
    == demand_df.columns.tolist()
)

assert demand_profile[
    "duplicate_key_count"
] == 0

assert demand_profile[
    "invalid_date_count"
] == 0

assert demand_profile[
    "missing_date_count"
] == 0

assert demand_profile[
    "historical_incomplete_day_count"
] == 0

for column in (
    "Hour",
    "Market Demand",
    "Ontario Demand",
):
    assert (
        demand_profile[
            "numeric_profile"
        ][column][
            "non_numeric_or_null_count"
        ]
        == 0
    )

print(
    "Reusable discovery framework validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "discovery_results" not in globals():
    discovery_results = {}

discovery_results[
    HOURLY_DEMAND_CONFIG.source_name
] = demand_profile

print(
    "Registered discovery profiles:",
    list(discovery_results.keys()),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source 02 — IESO Hourly Zonal Demand
# 
# **Business purpose:** Compare hourly electricity demand across Ontario electrical zones.
# 
# **Official source:** IESO Hourly Zonal Demand Report
# 
# **Discovery objectives:**
# - Preserve the raw source payload
# - Inspect the actual physical schema
# - Validate the source-level grain
# - Identify zone representation
# - Profile nulls, dates and numeric fields
# - Determine the normalization required for Silver
# - Collect evidence for the future data contract

# CELL ********************

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.request import Request, urlopen


ZONAL_SOURCE_CONFIG = {
    "source_name": "ieso_hourly_zonal_demand",
    "source_directory_url": (
        "https://reports-public.ieso.ca/public/DemandZonal/"
    ),
    "source_url": (
        "https://reports-public.ieso.ca/public/"
        "DemandZonal/PUB_DemandZonal_2026.csv"
    ),
    "source_file_name": "PUB_DemandZonal_2026.csv",
    "market_year": 2026,
    "bronze_directory": "Files/bronze/ieso/demand_zonal",
}


request = Request(
    ZONAL_SOURCE_CONFIG["source_url"],
    headers={
        "User-Agent": "GridPulseAI/1.0 SourceDiscovery"
    },
)

with urlopen(request, timeout=60) as response:
    zonal_http_status = response.status
    zonal_content_type = response.headers.get("Content-Type")
    zonal_raw_bytes = response.read()

assert zonal_http_status == 200, (
    f"Unexpected HTTP status: {zonal_http_status}"
)

assert len(zonal_raw_bytes) > 0, (
    "Source file is empty."
)

zonal_source_hash = sha256(
    zonal_raw_bytes
).hexdigest()

zonal_raw_text = zonal_raw_bytes.decode(
    "utf-8"
)

print(
    f"Source name       : "
    f"{ZONAL_SOURCE_CONFIG['source_name']}"
)
print(f"HTTP status       : {zonal_http_status}")
print(f"Content type      : {zonal_content_type}")
print(
    f"File size (bytes) : "
    f"{len(zonal_raw_bytes):,}"
)
print(f"SHA-256           : {zonal_source_hash}")
print(
    f"Retrieved at UTC  : "
    f"{datetime.now(timezone.utc).isoformat()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_hash_short = zonal_source_hash[:12]

zonal_bronze_file_name = (
    "PUB_DemandZonal_2026"
    f"__sha256_{zonal_hash_short}.csv"
)

zonal_bronze_path = (
    f"{ZONAL_SOURCE_CONFIG['bronze_directory']}/"
    f"{zonal_bronze_file_name}"
)

zonal_bronze_local_path = (
    Path("/lakehouse/default")
    / zonal_bronze_path
)

zonal_bronze_local_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

if zonal_bronze_local_path.exists():
    print("Payload already exists in Bronze.")
else:
    with zonal_bronze_local_path.open("xb") as file:
        file.write(zonal_raw_bytes)

    print("New payload written to Bronze.")

print(f"Path: {zonal_bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_persisted_size = (
    zonal_bronze_local_path.stat().st_size
)

zonal_hasher = sha256()

with zonal_bronze_local_path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        zonal_hasher.update(chunk)

zonal_persisted_hash = zonal_hasher.hexdigest()

print(
    f"Source size    : {len(zonal_raw_bytes):,}"
)
print(
    f"Bronze size    : {zonal_persisted_size:,}"
)
print(f"Source SHA-256 : {zonal_source_hash}")
print(f"Bronze SHA-256 : {zonal_persisted_hash}")

assert (
    zonal_persisted_size
    == len(zonal_raw_bytes)
)

assert (
    zonal_persisted_hash
    == zonal_source_hash
)

print(
    "\nRaw payload integrity validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_source_lines = zonal_raw_text.splitlines()

print(
    "=== FIRST PHYSICAL LINES "
    "FROM ZONAL SOURCE ===\n"
)

for line_number, line in enumerate(
    zonal_source_lines[:12],
    start=1,
):
    print(f"{line_number:02d}: {line}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ZONAL_DEMAND_CONFIG = SourceDiscoveryConfig(
    source_name="ieso_hourly_zonal_demand",

    required_header_columns=(
        "Date",
        "Hour",
        "Ontario Demand",
    ),

    expected_columns=(
        "Date",
        "Hour",
        "Ontario Demand",
        "Northwest",
        "Northeast",
        "Ottawa",
        "East",
        "Toronto",
        "Essa",
        "Bruce",
        "Southwest",
        "Niagara",
        "West",
        "Zone Total",
        "Diff",
    ),

    candidate_grain=(
        "Date",
        "Hour",
    ),

    date_column="Date",

    numeric_columns=(
        "Hour",
        "Ontario Demand",
        "Northwest",
        "Northeast",
        "Ottawa",
        "East",
        "Toronto",
        "Essa",
        "Bruce",
        "Southwest",
        "Niagara",
        "West",
        "Zone Total",
        "Diff",
    ),

    observed_rows_per_complete_day=24,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_df, zonal_header_index = (
    parse_csv_source(
        raw_text=zonal_raw_text,
        config=ZONAL_DEMAND_CONFIG,
    )
)

zonal_profile = build_source_profile(
    dataframe=zonal_df,
    config=ZONAL_DEMAND_CONFIG,
    header_row_index=zonal_header_index,
)

print_source_profile(
    zonal_profile
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert zonal_profile[
    "duplicate_key_count"
] == 0

assert zonal_profile[
    "invalid_date_count"
] == 0

assert zonal_profile[
    "missing_date_count"
] == 0

print(
    "Zonal Demand structural validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

discovery_results[
    ZONAL_DEMAND_CONFIG.source_name
] = zonal_profile

print(
    "Registered discovery profiles:"
)

for source_name in discovery_results:
    print(f" - {source_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ZONE_COLUMNS = [
    "Northwest",
    "Northeast",
    "Ottawa",
    "East",
    "Toronto",
    "Essa",
    "Bruce",
    "Southwest",
    "Niagara",
    "West",
]

zonal_semantic_df = zonal_df.copy()

zonal_semantic_df["_calculated_zone_total"] = (
    zonal_semantic_df[ZONE_COLUMNS]
    .sum(axis=1)
)

zonal_semantic_df["_calculated_diff"] = (
    zonal_semantic_df["Zone Total"]
    - zonal_semantic_df["Ontario Demand"]
)

zone_total_mismatch = zonal_semantic_df[
    zonal_semantic_df["_calculated_zone_total"]
    != zonal_semantic_df["Zone Total"]
]

diff_mismatch = zonal_semantic_df[
    zonal_semantic_df["_calculated_diff"]
    != zonal_semantic_df["Diff"]
]

print("=== ZONAL SEMANTIC INVESTIGATION ===\n")

print(f"Rows checked                 : {len(zonal_semantic_df):,}")
print(f"Zone Total mismatches        : {len(zone_total_mismatch):,}")
print(f"Diff mismatches              : {len(diff_mismatch):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

diff_profile = zonal_semantic_df["Diff"].describe()

print("=== DIFF PROFILE ===\n")
print(diff_profile)

print(
    "\nRows where Diff = 0:",
    int((zonal_semantic_df["Diff"] == 0).sum())
)

print(
    "Rows where Diff > 0:",
    int((zonal_semantic_df["Diff"] > 0).sum())
)

print(
    "Rows where Diff < 0:",
    int((zonal_semantic_df["Diff"] < 0).sum())
)

print("\nLargest absolute differences:")

display(
    zonal_semantic_df[
        [
            "Date",
            "Hour",
            "Ontario Demand",
            "Zone Total",
            "Diff",
        ]
    ]
    .assign(
        abs_diff=lambda df: df["Diff"].abs()
    )
    .sort_values(
        "abs_diff",
        ascending=False,
    )
    .head(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_reconciliation = (
    demand_df[
        [
            "Date",
            "Hour",
            "Ontario Demand",
        ]
    ]
    .rename(
        columns={
            "Ontario Demand":
                "ontario_demand_hourly_report"
        }
    )
    .merge(
        zonal_df[
            [
                "Date",
                "Hour",
                "Ontario Demand",
            ]
        ].rename(
            columns={
                "Ontario Demand":
                    "ontario_demand_zonal_report"
            }
        ),
        on=["Date", "Hour"],
        how="inner",
        validate="one_to_one",
    )
)

demand_reconciliation["difference"] = (
    demand_reconciliation[
        "ontario_demand_hourly_report"
    ]
    - demand_reconciliation[
        "ontario_demand_zonal_report"
    ]
)

print("=== CROSS-SOURCE RECONCILIATION ===\n")

print(
    f"Overlapping records : "
    f"{len(demand_reconciliation):,}"
)

print(
    f"Exact matches       : "
    f"{(demand_reconciliation['difference'] == 0).sum():,}"
)

print(
    f"Mismatches          : "
    f"{(demand_reconciliation['difference'] != 0).sum():,}"
)

print(
    f"Maximum abs diff    : "
    f"{demand_reconciliation['difference'].abs().max()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reconciliation_mismatches = (
    demand_reconciliation[
        demand_reconciliation["difference"] != 0
    ]
)

if reconciliation_mismatches.empty:
    print(
        "\nOntario Demand cross-source reconciliation: PASS"
    )
else:
    print(
        "\nOntario Demand cross-source reconciliation: INVESTIGATE"
    )

    display(
        reconciliation_mismatches.head(20)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zonal_semantic_df["_zone_total_delta"] = (
    zonal_semantic_df["_calculated_zone_total"]
    - zonal_semantic_df["Zone Total"]
)

zonal_semantic_df["_diff_delta"] = (
    zonal_semantic_df["_calculated_diff"]
    - zonal_semantic_df["Diff"]
)

print("=== ZONE TOTAL DELTA DISTRIBUTION ===")
print(
    zonal_semantic_df["_zone_total_delta"]
    .value_counts()
    .sort_index()
)

print("\n=== DIFF DELTA DISTRIBUTION ===")
print(
    zonal_semantic_df["_diff_delta"]
    .value_counts()
    .sort_index()
)

print("\n=== MAXIMUM ABSOLUTE DELTAS ===")

print(
    "Zone Total max abs delta:",
    zonal_semantic_df[
        "_zone_total_delta"
    ].abs().max()
)

print(
    "Diff max abs delta:",
    zonal_semantic_df[
        "_diff_delta"
    ].abs().max()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reconciliation_exceptions = (
    demand_reconciliation[
        demand_reconciliation["difference"] != 0
    ]
    .copy()
)

display(reconciliation_exceptions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reconciliation_summary = {
    "records_compared": int(
        len(demand_reconciliation)
    ),
    "records_matching": int(
        (
            demand_reconciliation["difference"] == 0
        ).sum()
    ),
    "records_mismatching": int(
        (
            demand_reconciliation["difference"] != 0
        ).sum()
    ),
    "maximum_absolute_difference": int(
        demand_reconciliation[
            "difference"
        ].abs().max()
    ),
}

reconciliation_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data quality findings — Hourly Zonal Demand
# 
# - Source-level candidate grain `Date + Hour` is unique in the inspected 2026 extract.
# - No missing calendar dates were observed.
# - No null values were observed in the inspected columns.
# - The physical source uses a wide zonal representation.
# - The ten published zone columns do not always sum exactly to the published `Zone Total`.
# - Observed arithmetic differences are small and require source-semantic confirmation before being converted into hard data-quality rules.
# - `Diff` does not always equal the arithmetic result of the displayed integer values `Zone Total - Ontario Demand`.
# - These relationships are therefore treated as observed source behaviour rather than contractual formulas.
# - Cross-source reconciliation of `Ontario Demand` against the Hourly Demand Report found one discrepancy in the inspected overlapping dataset.
# - The discrepancy is preserved and flagged for investigation; source values are not automatically corrected.
# - Cross-source inconsistencies are treated separately from malformed-record quarantine.


# MARKDOWN ********************

# ### Reconciliation exception
# 
# `2026-03-20, Hour 1`
# 
# Hourly Demand Report:
# `Ontario Demand = 15,232`
# 
# Hourly Zonal Demand Report:
# `Ontario Demand = 13,962`
# 
# Observed difference:
# `1,270`
# 
# Root cause:
# `Not confirmed`
# 
# Engineering treatment:
# Preserve both source values, retain source lineage and raise a reconciliation quality finding rather than silently overwriting either source.

# MARKDOWN ********************

# ## Source 03 — IESO Generator Output by Fuel Type Hourly
# 
# **Business purpose:** Understand Ontario's generation mix during relevant market and demand periods.
# 
# **Official source:** IESO Generator Output by Fuel Type Hourly Report
# 
# **Discovery objectives:**
# - Preserve the exact XML source payload
# - Inspect namespaces and XML hierarchy
# - Discover actual source elements
# - Identify physical source grain
# - Identify fuel-type representation
# - Identify hourly output representation
# - Profile dates, records, nulls and duplicates only after understanding the XML structure
# - Determine the transformation required for Silver
# 
# **Important:** No XML field names or business grain are assumed before source inspection.

# CELL ********************

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.request import Request, urlopen


GENERATION_SOURCE_CONFIG = {
    "source_name": "ieso_generation_by_fuel_hourly",
    "source_directory_url": (
        "https://reports-public.ieso.ca/public/"
        "GenOutputbyFuelHourly/"
    ),
    "source_url": (
        "https://reports-public.ieso.ca/public/"
        "GenOutputbyFuelHourly/"
        "PUB_GenOutputbyFuelHourly_2026.xml"
    ),
    "source_file_name": (
        "PUB_GenOutputbyFuelHourly_2026.xml"
    ),
    "market_year": 2026,
    "bronze_directory": (
        "Files/bronze/ieso/generation"
    ),
}


request = Request(
    GENERATION_SOURCE_CONFIG["source_url"],
    headers={
        "User-Agent": (
            "GridPulseAI/1.0 SourceDiscovery"
        )
    },
)

with urlopen(request, timeout=120) as response:
    generation_http_status = response.status
    generation_content_type = (
        response.headers.get("Content-Type")
    )
    generation_raw_bytes = response.read()

assert generation_http_status == 200, (
    f"Unexpected HTTP status: "
    f"{generation_http_status}"
)

assert len(generation_raw_bytes) > 0, (
    "Source XML file is empty."
)

generation_source_hash = sha256(
    generation_raw_bytes
).hexdigest()

print(
    f"Source name       : "
    f"{GENERATION_SOURCE_CONFIG['source_name']}"
)

print(
    f"HTTP status       : "
    f"{generation_http_status}"
)

print(
    f"Content type      : "
    f"{generation_content_type}"
)

print(
    f"File size (bytes) : "
    f"{len(generation_raw_bytes):,}"
)

print(
    f"SHA-256           : "
    f"{generation_source_hash}"
)

print(
    f"Retrieved at UTC  : "
    f"{datetime.now(timezone.utc).isoformat()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_hash_short = (
    generation_source_hash[:12]
)

generation_bronze_file_name = (
    "PUB_GenOutputbyFuelHourly_2026"
    f"__sha256_{generation_hash_short}.xml"
)

generation_bronze_path = (
    f"{GENERATION_SOURCE_CONFIG['bronze_directory']}/"
    f"{generation_bronze_file_name}"
)

generation_bronze_local_path = (
    Path("/lakehouse/default")
    / generation_bronze_path
)

generation_bronze_local_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

if generation_bronze_local_path.exists():
    print(
        "Payload already exists in Bronze."
    )

else:
    with generation_bronze_local_path.open(
        "xb"
    ) as file:
        file.write(generation_raw_bytes)

    print(
        "New payload written to Bronze."
    )

print(
    f"Path: {generation_bronze_path}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_persisted_size = (
    generation_bronze_local_path
    .stat()
    .st_size
)

generation_hasher = sha256()

with generation_bronze_local_path.open(
    "rb"
) as file:
    while chunk := file.read(
        1024 * 1024
    ):
        generation_hasher.update(chunk)

generation_persisted_hash = (
    generation_hasher.hexdigest()
)

print(
    f"Source size    : "
    f"{len(generation_raw_bytes):,}"
)

print(
    f"Bronze size    : "
    f"{generation_persisted_size:,}"
)

print(
    f"Source SHA-256 : "
    f"{generation_source_hash}"
)

print(
    f"Bronze SHA-256 : "
    f"{generation_persisted_hash}"
)

assert (
    generation_persisted_size
    == len(generation_raw_bytes)
)

assert (
    generation_persisted_hash
    == generation_source_hash
)

print(
    "\nRaw XML payload integrity validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xml.etree.ElementTree as ET


generation_root = ET.fromstring(
    generation_raw_bytes
)

print("=== XML ROOT DISCOVERY ===\n")

print(
    f"Raw root tag   : "
    f"{generation_root.tag}"
)

print(
    f"Root attributes: "
    f"{generation_root.attrib}"
)

print(
    f"Direct children: "
    f"{len(list(generation_root)):,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from io import BytesIO


discovered_namespaces = {}

for event, namespace in ET.iterparse(
    BytesIO(generation_raw_bytes),
    events=("start-ns",),
):
    prefix, uri = namespace

    discovered_namespaces[
        prefix or "(default)"
    ] = uri


print("=== XML NAMESPACES ===\n")

if discovered_namespaces:
    for prefix, uri in (
        discovered_namespaces.items()
    ):
        print(
            f"{prefix:<15} -> {uri}"
        )
else:
    print(
        "No XML namespaces detected."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def local_xml_name(tag: str) -> str:
    """
    Return an XML tag name without its namespace
    for human-readable source discovery.
    """

    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== ROOT CHILD ELEMENTS ===\n"
)

for index, child in enumerate(
    list(generation_root)[:25],
    start=1,
):
    print(
        f"{index:02d}: "
        f"{local_xml_name(child.tag)}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from collections import Counter


xml_tag_counts = Counter(
    local_xml_name(element.tag)
    for element
    in generation_root.iter()
)


print(
    "=== XML ELEMENT FREQUENCY ===\n"
)

for tag, count in (
    xml_tag_counts.most_common(40)
):
    print(
        f"{tag:<35} "
        f"{count:>10,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def collect_xml_paths(
    element,
    current_path="",
    paths=None,
):
    """
    Collect unique XML element paths from
    the source document.
    """

    if paths is None:
        paths = set()

    element_name = local_xml_name(
        element.tag
    )

    path = (
        f"{current_path}/{element_name}"
        if current_path
        else element_name
    )

    paths.add(path)

    for child in element:
        collect_xml_paths(
            child,
            current_path=path,
            paths=paths,
        )

    return paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_xml_paths = sorted(
    collect_xml_paths(
        generation_root
    )
)

print(
    "=== DISCOVERED XML PATHS ===\n"
)

for path in generation_xml_paths:
    print(path)

print(
    f"\nUnique XML paths: "
    f"{len(generation_xml_paths)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_values_by_tag = {}

for element in generation_root.iter():
    tag = local_xml_name(
        element.tag
    )

    text = (
        element.text.strip()
        if element.text
        else None
    )

    if (
        text
        and tag not in sample_values_by_tag
    ):
        sample_values_by_tag[tag] = text


print(
    "=== FIRST OBSERVED VALUE BY TAG ===\n"
)

for tag in sorted(
    sample_values_by_tag
):
    value = sample_values_by_tag[tag]

    preview = (
        value[:120]
        + ("..." if len(value) > 120 else "")
    )

    print(
        f"{tag:<35} : {preview}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def print_xml_tree(
    element,
    depth=0,
    max_depth=4,
    max_children=5,
):
    """
    Print a bounded representation of an XML tree.
    """

    indent = "  " * depth

    tag = local_xml_name(
        element.tag
    )

    text = (
        element.text.strip()
        if element.text
        else ""
    )

    text_preview = (
        text[:80]
        if text
        else ""
    )

    print(
        f"{indent}{tag}"
        + (
            f": {text_preview}"
            if text_preview
            else ""
        )
    )

    if depth >= max_depth:
        return

    children = list(element)

    for child in children[:max_children]:
        print_xml_tree(
            child,
            depth=depth + 1,
            max_depth=max_depth,
            max_children=max_children,
        )

    if len(children) > max_children:
        print(
            f"{indent}  "
            f"... "
            f"{len(children) - max_children} "
            f"more children"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== BOUNDED XML TREE SAMPLE ===\n"
)

print_xml_tree(
    generation_root,
    max_depth=5,
    max_children=4,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

daily_data_count = xml_tag_counts["DailyData"]
hourly_data_count = xml_tag_counts["HourlyData"]
fuel_total_count = xml_tag_counts["FuelTotal"]
energy_value_count = xml_tag_counts["EnergyValue"]
output_count = xml_tag_counts["Output"]
output_quality_count = xml_tag_counts["OutputQuality"]

print("=== XML STRUCTURAL COUNTS ===\n")

print(f"DailyData       : {daily_data_count:,}")
print(f"HourlyData      : {hourly_data_count:,}")
print(f"FuelTotal       : {fuel_total_count:,}")
print(f"EnergyValue     : {energy_value_count:,}")
print(f"Output          : {output_count:,}")
print(f"OutputQuality   : {output_quality_count:,}")

print("\nDerived ratios:")

print(
    "HourlyData / DailyData     :",
    hourly_data_count / daily_data_count
)

print(
    "FuelTotal / HourlyData     :",
    fuel_total_count / hourly_data_count
)

print(
    "Missing Output elements    :",
    energy_value_count - output_count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

IESO_XML_NS = "http://www.ieso.ca/schema"


def ieso_tag(local_name: str) -> str:
    """
    Build a fully-qualified IESO XML tag.
    """

    return f"{{{IESO_XML_NS}}}{local_name}"


observed_fuel_types = sorted(
    {
        element.text.strip()
        for element in generation_root.iter(
            ieso_tag("Fuel")
        )
        if element.text
    }
)

print("=== OBSERVED FUEL TYPES ===\n")

for fuel in observed_fuel_types:
    print(f" - {fuel}")

print(
    f"\nDistinct fuel types: "
    f"{len(observed_fuel_types)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fuel_count_records = []

for daily_data in generation_root.findall(
    ".//" + ieso_tag("DailyData")
):
    day_element = daily_data.find(
        ieso_tag("Day")
    )

    day_value = (
        day_element.text.strip()
        if day_element is not None
        and day_element.text
        else None
    )

    for hourly_data in daily_data.findall(
        ieso_tag("HourlyData")
    ):
        hour_element = hourly_data.find(
            ieso_tag("Hour")
        )

        hour_value = (
            hour_element.text.strip()
            if hour_element is not None
            and hour_element.text
            else None
        )

        fuel_totals = hourly_data.findall(
            ieso_tag("FuelTotal")
        )

        fuel_count_records.append(
            {
                "Date": day_value,
                "Hour": hour_value,
                "fuel_count": len(fuel_totals),
            }
        )


fuel_count_df = pd.DataFrame(
    fuel_count_records
)

print("=== FUEL COUNT PER HOUR ===\n")

display(
    fuel_count_df["fuel_count"]
    .value_counts()
    .rename_axis("fuel_count")
    .reset_index(name="hour_count")
    .sort_values("fuel_count")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

first_fuel_totals = list(
    generation_root.iter(
        ieso_tag("FuelTotal")
    )
)[:3]


for index, fuel_total in enumerate(
    first_fuel_totals,
    start=1,
):
    print(
        f"\n=== FUEL TOTAL SAMPLE {index} ==="
    )

    print_xml_tree(
        fuel_total,
        max_depth=5,
        max_children=10,
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GENERATION_XSD_URL = (
    "https://reports-public.ieso.ca/"
    "docrefs/schema/"
    "GenOutputbyFuelHourly_r1.xsd"
)

xsd_request = Request(
    GENERATION_XSD_URL,
    headers={
        "User-Agent": (
            "GridPulseAI/1.0 SourceDiscovery"
        )
    },
)

with urlopen(
    xsd_request,
    timeout=60,
) as response:
    xsd_status = response.status
    generation_xsd_bytes = response.read()


assert xsd_status == 200
assert len(generation_xsd_bytes) > 0

print(f"XSD HTTP status : {xsd_status}")
print(
    f"XSD size        : "
    f"{len(generation_xsd_bytes):,} bytes"
)

generation_xsd_text = (
    generation_xsd_bytes.decode(
        "utf-8"
    )
)

print("\n=== XSD FIRST 40 LINES ===\n")

for index, line in enumerate(
    generation_xsd_text.splitlines()[:40],
    start=1,
):
    print(
        f"{index:02d}: {line}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

xsd_lines = (
    generation_xsd_text.splitlines()
)

SEARCH_TERMS = [
    "FuelTotal",
    "Fuel",
    "EnergyValue",
    "Output",
    "OutputQuality",
]

for search_term in SEARCH_TERMS:
    print(
        f"\n=== XSD MATCHES: "
        f"{search_term} ==="
    )

    matches = [
        (
            index,
            line
        )
        for index, line in enumerate(
            xsd_lines,
            start=1,
        )
        if search_term in line
    ]

    for index, line in matches[:20]:
        print(
            f"{index:04d}: "
            f"{line.strip()}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_records = []


for daily_data in generation_root.findall(
    ".//" + ieso_tag("DailyData")
):
    day_element = daily_data.find(
        ieso_tag("Day")
    )

    day_value = (
        day_element.text.strip()
        if day_element is not None
        and day_element.text
        else None
    )

    for hourly_data in daily_data.findall(
        ieso_tag("HourlyData")
    ):
        hour_element = hourly_data.find(
            ieso_tag("Hour")
        )

        hour_value = (
            hour_element.text.strip()
            if hour_element is not None
            and hour_element.text
            else None
        )

        for fuel_total in hourly_data.findall(
            ieso_tag("FuelTotal")
        ):
            fuel_element = fuel_total.find(
                ieso_tag("Fuel")
            )

            energy_value = fuel_total.find(
                ieso_tag("EnergyValue")
            )

            output_element = (
                energy_value.find(
                    ieso_tag("Output")
                )
                if energy_value is not None
                else None
            )

            quality_element = (
                energy_value.find(
                    ieso_tag("OutputQuality")
                )
                if energy_value is not None
                else None
            )

            generation_records.append(
                {
                    "Date": day_value,
                    "Hour": hour_value,
                    "Fuel": (
                        fuel_element.text.strip()
                        if fuel_element is not None
                        and fuel_element.text
                        else None
                    ),
                    "Output": (
                        output_element.text.strip()
                        if output_element is not None
                        and output_element.text
                        else None
                    ),
                    "OutputQuality": (
                        quality_element.text.strip()
                        if quality_element is not None
                        and quality_element.text
                        else None
                    ),
                }
            )


generation_df = pd.DataFrame(
    generation_records
)

print(
    f"Flattened records: "
    f"{len(generation_df):,}"
)

display(
    generation_df.head(15)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_df["_parsed_date"] = (
    pd.to_datetime(
        generation_df["Date"],
        errors="coerce",
    )
)

generation_df["_numeric_hour"] = (
    pd.to_numeric(
        generation_df["Hour"],
        errors="coerce",
    )
)

generation_df["_numeric_output"] = (
    pd.to_numeric(
        generation_df["Output"],
        errors="coerce",
    )
)


GENERATION_GRAIN = [
    "Date",
    "Hour",
    "Fuel",
]


generation_duplicate_mask = (
    generation_df.duplicated(
        subset=GENERATION_GRAIN,
        keep=False,
    )
)

generation_duplicate_rows = (
    generation_df[
        generation_duplicate_mask
    ]
)


print(
    "=== GENERATION SOURCE PROFILE ===\n"
)

print(
    f"Rows                  : "
    f"{len(generation_df):,}"
)

print(
    f"Minimum date          : "
    f"{generation_df['_parsed_date'].min()}"
)

print(
    f"Maximum date          : "
    f"{generation_df['_parsed_date'].max()}"
)

print(
    f"Minimum hour          : "
    f"{generation_df['_numeric_hour'].min()}"
)

print(
    f"Maximum hour          : "
    f"{generation_df['_numeric_hour'].max()}"
)

print(
    f"Distinct fuels        : "
    f"{generation_df['Fuel'].nunique()}"
)

print(
    f"Duplicate grain rows  : "
    f"{len(generation_duplicate_rows):,}"
)

print("\nNull counts:")

print(
    generation_df[
        [
            "Date",
            "Hour",
            "Fuel",
            "Output",
            "OutputQuality",
        ]
    ].isna().sum()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing_generation_output = (
    generation_df[
        generation_df["Output"].isna()
    ][
        [
            "Date",
            "Hour",
            "Fuel",
            "Output",
            "OutputQuality",
        ]
    ]
    .copy()
)


print(
    "=== MISSING GENERATION OUTPUT ===\n"
)

print(
    f"Records: "
    f"{len(missing_generation_output)}"
)

display(
    missing_generation_output
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== OUTPUT QUALITY DISTRIBUTION ===\n"
)

display(
    generation_df[
        "OutputQuality"
    ]
    .value_counts(
        dropna=False
    )
    .rename_axis(
        "OutputQuality"
    )
    .reset_index(
        name="record_count"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== OUTPUT QUALITY VS MISSING OUTPUT ===\n"
)

quality_missing_analysis = (
    generation_df
    .assign(
        output_missing=(
            generation_df["Output"].isna()
        )
    )
    .groupby(
        [
            "OutputQuality",
            "output_missing",
        ],
        dropna=False,
    )
    .size()
    .reset_index(
        name="record_count"
    )
)

display(
    quality_missing_analysis
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eight_fuel_hours = (
    fuel_count_df[
        fuel_count_df["fuel_count"] == 8
    ]
    .copy()
)

print("=== HOURS WITH 8 FUEL TYPES ===\n")

print(
    f"Records: {len(eight_fuel_hours):,}"
)

display(eight_fuel_hours)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control_actions_df = (
    generation_df[
        generation_df["Fuel"]
        == "CONTROL ACTIONS"
    ][
        [
            "Date",
            "Hour",
            "Fuel",
            "Output",
            "OutputQuality",
        ]
    ]
    .copy()
)

print("=== CONTROL ACTIONS RECORDS ===\n")

print(
    f"Records: {len(control_actions_df):,}"
)

print(
    f"Distinct dates: "
    f"{control_actions_df['Date'].nunique():,}"
)

display(control_actions_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_20260715 = (
    generation_df[
        generation_df["Date"]
        == "2026-07-15"
    ][
        [
            "Date",
            "Hour",
            "Fuel",
            "Output",
            "OutputQuality",
        ]
    ]
    .copy()
)

generation_20260715["Hour_numeric"] = (
    pd.to_numeric(
        generation_20260715["Hour"],
        errors="coerce",
    )
)

display(
    generation_20260715
    .sort_values(
        ["Hour_numeric", "Fuel"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    control_actions_df
    .assign(
        Hour_numeric=lambda df:
            pd.to_numeric(
                df["Hour"],
                errors="coerce",
            )
    )
    .sort_values(
        ["Date", "Hour_numeric"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== RELEVANT XSD SECTION ===\n")

for line_number in range(39, 64):
    if line_number <= len(xsd_lines):
        print(
            f"{line_number:04d}: "
            f"{xsd_lines[line_number - 1]}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GENERATION_CONFIG = SourceDiscoveryConfig(
    source_name="ieso_generation_by_fuel_hourly",

    required_header_columns=(),

    expected_columns=(
        "Date",
        "Hour",
        "Fuel",
        "Output",
        "OutputQuality",
    ),

    candidate_grain=(
        "Date",
        "Hour",
        "Fuel",
    ),

    date_column="Date",

    numeric_columns=(
        "Hour",
        "Output",
        "OutputQuality",
    ),

    observed_rows_per_complete_day=None,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generation_profile_df = (
    generation_df[
        [
            "Date",
            "Hour",
            "Fuel",
            "Output",
            "OutputQuality",
        ]
    ]
    .copy()
)

generation_profile = build_source_profile(
    dataframe=generation_profile_df,
    config=GENERATION_CONFIG,
    header_row_index=-1,
)

print_source_profile(
    generation_profile
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert generation_profile[
    "duplicate_key_count"
] == 0

assert generation_profile[
    "invalid_date_count"
] == 0

discovery_results[
    GENERATION_CONFIG.source_name
] = generation_profile

print(
    "Registered discovery profiles:"
)

for source_name in discovery_results:
    print(f" - {source_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Source 04 — IESO Day-Ahead Ontario Zonal Price
# 
# **Business purpose:** Analyze the hourly Day-Ahead Ontario Zonal Price and compare it with real-time market prices.
# 
# **Official source:** IESO Day-Ahead Hourly Ontario Zonal Energy Price Report
# 
# **Discovery objectives:**
# - Validate historical source availability for the 2026 MVP period
# - Preserve raw daily XML payloads
# - Inspect the actual XML contract
# - Discover price component elements
# - Validate hourly grain
# - Validate documented price-component reconciliation
# - Understand source revisions and daily publication behaviour

# CELL ********************

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DA_PRICE_BASE_URL = (
    "https://reports-public.ieso.ca/public/"
    "DAHourlyOntarioZonalPrice"
)


def probe_source_url(url: str) -> dict:
    """
    Check whether an IESO source URL is currently accessible
    without persisting the payload.
    """

    request = Request(
        url,
        headers={
            "User-Agent": "GridPulseAI/1.0 SourceDiscovery"
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            payload = response.read()

            return {
                "url": url,
                "status": response.status,
                "available": response.status == 200,
                "size_bytes": len(payload),
                "content_type": response.headers.get(
                    "Content-Type"
                ),
            }

    except HTTPError as exc:
        return {
            "url": url,
            "status": exc.code,
            "available": False,
            "size_bytes": None,
            "content_type": None,
        }

    except URLError as exc:
        return {
            "url": url,
            "status": None,
            "available": False,
            "size_bytes": None,
            "content_type": None,
            "error": str(exc.reason),
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_AVAILABILITY_TEST_DATES = [
    "20260101",
    "20260201",
    "20260301",
    "20260401",
    "20260501",
    "20260518",
    "20260717",
    "20260816",
]


availability_results = []

for date_value in DA_AVAILABILITY_TEST_DATES:
    file_name = (
        "PUB_DAHourlyOntarioZonalPrice_"
        f"{date_value}.xml"
    )

    url = (
        f"{DA_PRICE_BASE_URL}/"
        f"{file_name}"
    )

    result = probe_source_url(url)

    availability_results.append(
        {
            "dispatch_date": date_value,
            "status": result["status"],
            "available": result["available"],
            "size_bytes": result["size_bytes"],
        }
    )


availability_df = pd.DataFrame(
    availability_results
)

display(availability_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_LATEST_URL = (
    f"{DA_PRICE_BASE_URL}/"
    "PUB_DAHourlyOntarioZonalPrice.xml"
)

latest_da_probe = probe_source_url(
    DA_LATEST_URL
)

latest_da_probe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def probe_daily_report_versions(
    base_url: str,
    file_prefix: str,
    date_value: str,
    max_version: int = 5,
) -> pd.DataFrame:
    """
    Probe the base daily report and a bounded set of
    versioned filenames without persisting any payload.
    """

    candidate_names = [
        f"{file_prefix}_{date_value}.xml"
    ]

    candidate_names.extend(
        f"{file_prefix}_{date_value}_v{version}.xml"
        for version in range(1, max_version + 1)
    )

    results = []

    for file_name in candidate_names:
        url = f"{base_url}/{file_name}"

        probe = probe_source_url(url)

        results.append(
            {
                "dispatch_date": date_value,
                "file_name": file_name,
                "status": probe.get("status"),
                "available": probe.get("available"),
                "size_bytes": probe.get("size_bytes"),
            }
        )

    return pd.DataFrame(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_VERSION_TEST_DATES = [
    "20260324",
    "20260501",
    "20260517",
    "20260518",
    "20260717",
]

version_probe_results = []

for date_value in DA_VERSION_TEST_DATES:
    result_df = probe_daily_report_versions(
        base_url=DA_PRICE_BASE_URL,
        file_prefix="PUB_DAHourlyOntarioZonalPrice",
        date_value=date_value,
        max_version=5,
    )

    version_probe_results.append(result_df)


da_version_availability_df = pd.concat(
    version_probe_results,
    ignore_index=True,
)

display(
    da_version_availability_df[
        da_version_availability_df["available"]
    ].sort_values(
        ["dispatch_date", "file_name"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    da_version_availability_df.sort_values(
        ["dispatch_date", "file_name"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Session bootstrap — resumed 2026-08-25

# CELL ********************

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.request import Request, urlopen
from collections import Counter
from io import BytesIO

import xml.etree.ElementTree as ET
import pandas as pd


def local_xml_name(tag: str) -> str:
    """
    Return an XML tag name without its namespace
    for human-readable source discovery.
    """

    if "}" in tag:
        return tag.split("}", 1)[1]

    return tag


def collect_xml_paths(
    element,
    current_path="",
    paths=None,
):
    """
    Collect unique XML element paths from
    the source document.
    """

    if paths is None:
        paths = set()

    element_name = local_xml_name(
        element.tag
    )

    path = (
        f"{current_path}/{element_name}"
        if current_path
        else element_name
    )

    paths.add(path)

    for child in element:
        collect_xml_paths(
            child,
            current_path=path,
            paths=paths,
        )

    return paths


def print_xml_tree(
    element,
    depth=0,
    max_depth=4,
    max_children=5,
):
    """
    Print a bounded representation of an XML tree.
    """

    indent = "  " * depth

    tag = local_xml_name(
        element.tag
    )

    text = (
        element.text.strip()
        if element.text
        else ""
    )

    text_preview = (
        text[:80]
        if text
        else ""
    )

    print(
        f"{indent}{tag}"
        + (
            f": {text_preview}"
            if text_preview
            else ""
        )
    )

    if depth >= max_depth:
        return

    children = list(element)

    for child in children[:max_children]:
        print_xml_tree(
            child,
            depth=depth + 1,
            max_depth=max_depth,
            max_children=max_children,
        )

    if len(children) > max_children:
        print(
            f"{indent}  ... "
            f"{len(children) - max_children} more children"
        )


print("GridPulse discovery session bootstrap: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_SAMPLE_CONFIG = {
    "source_name": "ieso_day_ahead_ontario_zonal_price",
    "dispatch_date": "20260816",
    "source_url": (
        "https://reports-public.ieso.ca/public/"
        "DAHourlyOntarioZonalPrice/"
        "PUB_DAHourlyOntarioZonalPrice_20260816.xml"
    ),
    "source_file_name": (
        "PUB_DAHourlyOntarioZonalPrice_20260816.xml"
    ),
    "bronze_directory": (
        "Files/bronze/ieso/price_day_ahead"
    ),
}


request = Request(
    DA_SAMPLE_CONFIG["source_url"],
    headers={
        "User-Agent": "GridPulseAI/1.0 SourceDiscovery"
    },
)

with urlopen(request, timeout=60) as response:
    da_http_status = response.status
    da_content_type = response.headers.get(
        "Content-Type"
    )
    da_raw_bytes = response.read()

assert da_http_status == 200, (
    f"Unexpected HTTP status: {da_http_status}"
)

assert len(da_raw_bytes) > 0, (
    "Day-Ahead source file is empty."
)

da_source_hash = sha256(
    da_raw_bytes
).hexdigest()

print(
    f"Source name       : "
    f"{DA_SAMPLE_CONFIG['source_name']}"
)
print(
    f"Dispatch date     : "
    f"{DA_SAMPLE_CONFIG['dispatch_date']}"
)
print(f"HTTP status       : {da_http_status}")
print(f"Content type      : {da_content_type}")
print(
    f"File size (bytes) : "
    f"{len(da_raw_bytes):,}"
)
print(f"SHA-256           : {da_source_hash}")
print(
    f"Retrieved at UTC  : "
    f"{datetime.now(timezone.utc).isoformat()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_hash_short = da_source_hash[:12]

da_bronze_file_name = (
    "PUB_DAHourlyOntarioZonalPrice_20260816"
    f"__sha256_{da_hash_short}.xml"
)

da_bronze_path = (
    f"{DA_SAMPLE_CONFIG['bronze_directory']}/"
    f"{da_bronze_file_name}"
)

da_bronze_local_path = (
    Path("/lakehouse/default")
    / da_bronze_path
)

da_bronze_local_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

if da_bronze_local_path.exists():
    print("Payload already exists in Bronze.")

else:
    with da_bronze_local_path.open("xb") as file:
        file.write(da_raw_bytes)

    print("New payload written to Bronze.")

print(f"Path: {da_bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_persisted_size = (
    da_bronze_local_path.stat().st_size
)

da_hasher = sha256()

with da_bronze_local_path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        da_hasher.update(chunk)

da_persisted_hash = da_hasher.hexdigest()

print(f"Source size    : {len(da_raw_bytes):,}")
print(f"Bronze size    : {da_persisted_size:,}")
print(f"Source SHA-256 : {da_source_hash}")
print(f"Bronze SHA-256 : {da_persisted_hash}")

assert da_persisted_size == len(da_raw_bytes)

assert da_persisted_hash == da_source_hash

print(
    "\nRaw DA XML payload integrity validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xml.etree.ElementTree as ET
from collections import Counter
from io import BytesIO


da_root = ET.fromstring(
    da_raw_bytes
)

print("=== DA XML ROOT DISCOVERY ===\n")

print(f"Raw root tag    : {da_root.tag}")
print(f"Root attributes : {da_root.attrib}")
print(
    f"Direct children : "
    f"{len(list(da_root))}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_namespaces = {}

for event, namespace in ET.iterparse(
    BytesIO(da_raw_bytes),
    events=("start-ns",),
):
    prefix, uri = namespace

    da_namespaces[
        prefix or "(default)"
    ] = uri


print("=== DA XML NAMESPACES ===\n")

if da_namespaces:
    for prefix, uri in da_namespaces.items():
        print(
            f"{prefix:<15} -> {uri}"
        )
else:
    print("No namespaces detected.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_tag_counts = Counter(
    local_xml_name(element.tag)
    for element in da_root.iter()
)


print("=== DA XML ELEMENT FREQUENCY ===\n")

for tag, count in (
    da_tag_counts.most_common(30)
):
    print(
        f"{tag:<40} {count:>8,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_xml_paths = sorted(
    collect_xml_paths(
        da_root
    )
)

print("=== DA XML PATHS ===\n")

for path in da_xml_paths:
    print(path)

print(
    f"\nUnique XML paths: "
    f"{len(da_xml_paths)}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== DA BOUNDED XML TREE ===\n")

print_xml_tree(
    da_root,
    max_depth=6,
    max_children=8,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

XSI_SCHEMA_LOCATION = (
    "{http://www.w3.org/2001/XMLSchema-instance}"
    "schemaLocation"
)

da_schema_location = da_root.attrib.get(
    XSI_SCHEMA_LOCATION
)

print(
    "=== DECLARED XSD LOCATION ===\n"
)

print(da_schema_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_xsd_url = None

if da_schema_location:
    schema_parts = (
        da_schema_location
        .strip()
        .split()
    )

    if len(schema_parts) >= 2:
        da_xsd_url = schema_parts[-1]

print(
    f"Resolved XSD URL: "
    f"{da_xsd_url}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert da_xsd_url is not None, (
    "No XSD URL was declared by the source XML."
)

xsd_request = Request(
    da_xsd_url,
    headers={
        "User-Agent": (
            "GridPulseAI/1.0 SourceDiscovery"
        )
    },
)

with urlopen(
    xsd_request,
    timeout=60,
) as response:
    da_xsd_status = response.status
    da_xsd_bytes = response.read()


assert da_xsd_status == 200
assert len(da_xsd_bytes) > 0

da_xsd_text = da_xsd_bytes.decode(
    "utf-8"
)

print(
    f"XSD HTTP status : "
    f"{da_xsd_status}"
)

print(
    f"XSD size        : "
    f"{len(da_xsd_bytes):,} bytes"
)

print(
    "\n=== XSD FIRST 50 LINES ===\n"
)

for index, line in enumerate(
    da_xsd_text.splitlines()[:50],
    start=1,
):
    print(
        f"{index:02d}: {line}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

da_xsd_lines = (
    da_xsd_text.splitlines()
)

SEARCH_TERMS = [
    "Price",
    "Hour",
    "Date",
    "Reference",
    "Loss",
    "Congestion",
    "Zonal",
]

for search_term in SEARCH_TERMS:
    print(
        f"\n=== XSD MATCHES: "
        f"{search_term} ==="
    )

    matches = [
        (index, line)
        for index, line in enumerate(
            da_xsd_lines,
            start=1,
        )
        if search_term.lower()
        in line.lower()
    ]

    for index, line in matches[:30]:
        print(
            f"{index:04d}: "
            f"{line.strip()}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== DA XSD REMAINING SECTION ===\n")

for line_number in range(51, len(da_xsd_lines) + 1):
    print(
        f"{line_number:04d}: "
        f"{da_xsd_lines[line_number - 1]}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from decimal import Decimal, InvalidOperation


DA_XML_NS = "http://www.ieso.ca/schema"


def da_tag(local_name: str) -> str:
    """
    Build a fully-qualified IESO XML tag.
    """

    return f"{{{DA_XML_NS}}}{local_name}"


def get_xml_text(
    parent,
    local_name: str,
):
    """
    Return stripped element text or None.
    """

    element = parent.find(
        da_tag(local_name)
    )

    if (
        element is None
        or element.text is None
        or not element.text.strip()
    ):
        return None

    return element.text.strip()


def parse_optional_decimal(
    value,
):
    """
    Parse a decimal value while preserving source nullability.
    """

    if value is None:
        return None

    try:
        return Decimal(value)

    except InvalidOperation as exc:
        raise ValueError(
            f"Invalid decimal value: {value}"
        ) from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delivery_date = get_xml_text(
    da_root.find(
        da_tag("DocBody")
    ),
    "DeliveryDate",
)

assert delivery_date is not None, (
    "DeliveryDate is missing from DA report."
)


da_price_records = []

hourly_components = da_root.findall(
    ".//" + da_tag("HourlyPriceComponents")
)


for component in hourly_components:

    pricing_hour = get_xml_text(
        component,
        "PricingHour",
    )

    zonal_price = get_xml_text(
        component,
        "ZonalPrice",
    )

    loss_price = get_xml_text(
        component,
        "LossPriceCapped",
    )

    congestion_price = get_xml_text(
        component,
        "CongestionPriceCapped",
    )

    flag = get_xml_text(
        component,
        "Flag",
    )

    da_price_records.append(
        {
            "DeliveryDate": delivery_date,
            "PricingHour": (
                int(pricing_hour)
                if pricing_hour is not None
                else None
            ),
            "ZonalPrice": (
                parse_optional_decimal(
                    zonal_price
                )
            ),
            "LossPriceCapped": (
                parse_optional_decimal(
                    loss_price
                )
            ),
            "CongestionPriceCapped": (
                parse_optional_decimal(
                    congestion_price
                )
            ),
            "Flag": flag,
        }
    )


da_price_df = pd.DataFrame(
    da_price_records
)


print(
    f"Flattened DA price records: "
    f"{len(da_price_df):,}"
)

display(
    da_price_df.head(24)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_GRAIN = [
    "DeliveryDate",
    "PricingHour",
]


duplicate_mask = (
    da_price_df.duplicated(
        subset=DA_GRAIN,
        keep=False,
    )
)

duplicate_rows = (
    da_price_df[
        duplicate_mask
    ]
)


print("=== DA CANDIDATE GRAIN ===\n")

print(
    f"Total rows      : "
    f"{len(da_price_df):,}"
)

print(
    f"Unique keys     : "
    f"{da_price_df[DA_GRAIN].drop_duplicates().shape[0]:,}"
)

print(
    f"Duplicate rows  : "
    f"{len(duplicate_rows):,}"
)


if duplicate_rows.empty:
    print(
        "\nCandidate grain validation: PASS"
    )
else:
    print(
        "\nCandidate grain validation: INVESTIGATE"
    )

    display(duplicate_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

observed_hours = set(
    da_price_df[
        "PricingHour"
    ].dropna()
)

expected_hours = set(
    range(1, 25)
)


print("=== DA HOUR COVERAGE ===\n")

print(
    f"Observed hours : "
    f"{sorted(observed_hours)}"
)

print(
    f"Missing hours  : "
    f"{sorted(expected_hours - observed_hours)}"
)

print(
    f"Unexpected     : "
    f"{sorted(observed_hours - expected_hours)}"
)


assert (
    observed_hours
    == expected_hours
), (
    "DA report does not contain the expected "
    "PricingHour domain 1–24."
)

print(
    "\nDA hour coverage validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== DA PRICE PROFILE ===\n"
)

print(
    f"Delivery date : "
    f"{da_price_df['DeliveryDate'].iloc[0]}"
)

print(
    f"Rows          : "
    f"{len(da_price_df):,}"
)

print("\nNull counts:")

print(
    da_price_df.isna().sum()
)


print("\nFlag distribution:")

display(
    da_price_df[
        "Flag"
    ]
    .value_counts(
        dropna=False
    )
    .rename_axis(
        "Flag"
    )
    .reset_index(
        name="record_count"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PRICE_COLUMNS = [
    "ZonalPrice",
    "LossPriceCapped",
    "CongestionPriceCapped",
]


for column in PRICE_COLUMNS:

    values = [
        value
        for value in da_price_df[column]
        if value is not None
    ]

    print(
        f"\n=== {column} ==="
    )

    print(
        f"Non-null records : "
        f"{len(values):,}"
    )

    if values:
        print(
            f"Minimum          : "
            f"{min(values)}"
        )

        print(
            f"Maximum          : "
            f"{max(values)}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== OBSERVED DA FLAGS ===\n"
)

for flag in sorted(
    da_price_df[
        "Flag"
    ]
    .dropna()
    .unique()
):
    print(
        f" - {flag}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Discovery findings — Day-Ahead Ontario Zonal Price
# 
# - Source format: XML.
# - Report grain observed: `DeliveryDate + PricingHour`.
# - The inspected report contains 24 unique hourly records covering PricingHour 1–24.
# - `ZonalPrice`, `LossPriceCapped`, and `CongestionPriceCapped` are defined by the source XSD as `empty_or_decimal`.
# - Price fields are therefore nullable by contract even though no null values were observed in the inspected sample.
# - Negative values are observed in loss and congestion components and must not be treated as invalid by default.
# - `Flag` is preserved as source metadata; the inspected sample contains only `DSO-RD`.
# - Day-Ahead historical availability is source-specific and shorter than the intended 2026 analytical period.
# - Source revisions exist and must be preserved through version/hash-aware ingestion.

# MARKDOWN ********************

# ## Source 05 — IESO Real-Time Ontario Zonal Price
# 
# **Business purpose:** Analyze five-minute real-time Ontario electricity prices and compare them with Day-Ahead prices.
# 
# **Official source:** IESO Real-time 5-min Ontario Zonal Energy Price Report
# 
# **Discovery objectives:**
# - Preserve a real source snapshot
# - Inspect the mutable latest-report behaviour
# - Discover the actual XML hierarchy
# - Identify dispatch date, dispatch hour and interval representation
# - Discover price and quality/flag fields
# - Determine the physical source grain
# - Gather evidence for the Eventstream event contract
# - Determine revision and deduplication requirements

# CELL ********************

RT_SOURCE_CONFIG = {
    "source_name": "ieso_realtime_ontario_zonal_price",
    "source_url": (
        "https://reports-public.ieso.ca/public/"
        "RealtimeOntarioZonalPrice/"
        "PUB_RealtimeOntarioZonalPrice.xml"
    ),
    "source_file_name": (
        "PUB_RealtimeOntarioZonalPrice.xml"
    ),
    "bronze_directory": (
        "Files/bronze/ieso/price_realtime"
    ),
}


request = Request(
    RT_SOURCE_CONFIG["source_url"],
    headers={
        "User-Agent": "GridPulseAI/1.0 SourceDiscovery"
    },
)

with urlopen(request, timeout=60) as response:
    rt_http_status = response.status
    rt_content_type = response.headers.get(
        "Content-Type"
    )
    rt_raw_bytes = response.read()

assert rt_http_status == 200, (
    f"Unexpected HTTP status: {rt_http_status}"
)

assert len(rt_raw_bytes) > 0, (
    "Real-Time source file is empty."
)

rt_source_hash = sha256(
    rt_raw_bytes
).hexdigest()

rt_retrieved_at = datetime.now(
    timezone.utc
)

print(
    f"Source name       : "
    f"{RT_SOURCE_CONFIG['source_name']}"
)
print(f"HTTP status       : {rt_http_status}")
print(f"Content type      : {rt_content_type}")
print(
    f"File size (bytes) : "
    f"{len(rt_raw_bytes):,}"
)
print(f"SHA-256           : {rt_source_hash}")
print(
    f"Retrieved at UTC  : "
    f"{rt_retrieved_at.isoformat()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_hash_short = rt_source_hash[:12]

rt_retrieval_key = (
    rt_retrieved_at
    .strftime("%Y%m%dT%H%M%SZ")
)

rt_bronze_file_name = (
    "PUB_RealtimeOntarioZonalPrice"
    f"__retrieved_{rt_retrieval_key}"
    f"__sha256_{rt_hash_short}.xml"
)

rt_bronze_path = (
    f"{RT_SOURCE_CONFIG['bronze_directory']}/"
    f"{rt_bronze_file_name}"
)

rt_bronze_local_path = (
    Path("/lakehouse/default")
    / rt_bronze_path
)

rt_bronze_local_path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

if rt_bronze_local_path.exists():
    print("Payload already exists in Bronze.")

else:
    with rt_bronze_local_path.open("xb") as file:
        file.write(rt_raw_bytes)

    print("New RT snapshot written to Bronze.")

print(f"Path: {rt_bronze_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_hasher = sha256()

with rt_bronze_local_path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        rt_hasher.update(chunk)

rt_persisted_hash = rt_hasher.hexdigest()

print(
    f"Source size    : "
    f"{len(rt_raw_bytes):,}"
)
print(
    f"Bronze size    : "
    f"{rt_bronze_local_path.stat().st_size:,}"
)
print(f"Source SHA-256 : {rt_source_hash}")
print(f"Bronze SHA-256 : {rt_persisted_hash}")

assert (
    rt_persisted_hash
    == rt_source_hash
)

print(
    "\nRaw RT XML payload integrity validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_root = ET.fromstring(
    rt_raw_bytes
)

print("=== RT XML ROOT DISCOVERY ===\n")

print(f"Raw root tag    : {rt_root.tag}")
print(f"Root attributes : {rt_root.attrib}")
print(
    f"Direct children : "
    f"{len(list(rt_root))}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_namespaces = {}

for event, namespace in ET.iterparse(
    BytesIO(rt_raw_bytes),
    events=("start-ns",),
):
    prefix, uri = namespace

    rt_namespaces[
        prefix or "(default)"
    ] = uri


print("=== RT XML NAMESPACES ===\n")

for prefix, uri in rt_namespaces.items():
    print(
        f"{prefix:<15} -> {uri}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_tag_counts = Counter(
    local_xml_name(element.tag)
    for element in rt_root.iter()
)

print("=== RT XML ELEMENT FREQUENCY ===\n")

for tag, count in (
    rt_tag_counts.most_common(30)
):
    print(
        f"{tag:<40} {count:>8,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_xml_paths = sorted(
    collect_xml_paths(
        rt_root
    )
)

print("=== RT XML PATHS ===\n")

for path in rt_xml_paths:
    print(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== RT BOUNDED XML TREE ===\n"
)

print_xml_tree(
    rt_root,
    max_depth=7,
    max_children=15,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

XSI_SCHEMA_LOCATION = (
    "{http://www.w3.org/2001/XMLSchema-instance}"
    "schemaLocation"
)

rt_schema_location = rt_root.attrib.get(
    XSI_SCHEMA_LOCATION
)

print("=== RT DECLARED XSD LOCATION ===\n")
print(rt_schema_location)


rt_xsd_url = None

if rt_schema_location:
    schema_parts = (
        rt_schema_location
        .strip()
        .split()
    )

    if len(schema_parts) >= 2:
        rt_xsd_url = schema_parts[-1]


print(
    f"\nResolved RT XSD URL: "
    f"{rt_xsd_url}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert rt_xsd_url is not None, (
    "No XSD URL was declared by the RT source XML."
)

rt_xsd_request = Request(
    rt_xsd_url,
    headers={
        "User-Agent": (
            "GridPulseAI/1.0 SourceDiscovery"
        )
    },
)

with urlopen(
    rt_xsd_request,
    timeout=60,
) as response:
    rt_xsd_status = response.status
    rt_xsd_bytes = response.read()


assert rt_xsd_status == 200
assert len(rt_xsd_bytes) > 0

rt_xsd_text = rt_xsd_bytes.decode(
    "utf-8"
)

rt_xsd_lines = (
    rt_xsd_text.splitlines()
)

print(
    f"XSD HTTP status : "
    f"{rt_xsd_status}"
)

print(
    f"XSD size        : "
    f"{len(rt_xsd_bytes):,} bytes"
)

print(
    "\n=== RT XSD ===\n"
)

for index, line in enumerate(
    rt_xsd_lines,
    start=1,
):
    print(
        f"{index:04d}: {line}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RT_SEARCH_TERMS = [
    "DeliveryDate",
    "DeliveryHour",
    "Interval",
    "ZonalPrice",
    "LmpCap",
    "LossPriceCap",
    "CongPriceCap",
    "Flag",
    "AveragePrice",
    "empty",
    "Money",
]


for search_term in RT_SEARCH_TERMS:

    print(
        f"\n=== RT XSD MATCHES: "
        f"{search_term} ==="
    )

    matches = [
        (index, line)
        for index, line in enumerate(
            rt_xsd_lines,
            start=1,
        )
        if search_term.lower()
        in line.lower()
    ]

    for index, line in matches:
        print(
            f"{index:04d}: "
            f"{line.strip()}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from decimal import Decimal, InvalidOperation


RT_XML_NS = "http://www.ieso.ca/schema"


def rt_tag(local_name: str) -> str:
    """
    Build a fully-qualified IESO XML tag.
    """

    return f"{{{RT_XML_NS}}}{local_name}"


def get_rt_xml_text(
    parent,
    local_name: str,
):
    """
    Return stripped XML text or None when the element
    is missing or contains an empty value.
    """

    element = parent.find(
        rt_tag(local_name)
    )

    if (
        element is None
        or element.text is None
        or not element.text.strip()
    ):
        return None

    return element.text.strip()


def parse_rt_decimal(value):
    """
    Parse an optional RT price value as Decimal.
    """

    if value is None:
        return None

    try:
        return Decimal(value)

    except InvalidOperation as exc:
        raise ValueError(
            f"Invalid decimal value: {value}"
        ) from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_doc_body = rt_root.find(
    rt_tag("DocBody")
)

rt_doc_header = rt_root.find(
    rt_tag("DocHeader")
)

assert rt_doc_body is not None
assert rt_doc_header is not None


rt_delivery_date = get_rt_xml_text(
    rt_doc_body,
    "DeliveryDate",
)

rt_delivery_hour = get_rt_xml_text(
    rt_doc_body,
    "DeliveryHour",
)

rt_created_at = get_rt_xml_text(
    rt_doc_header,
    "CreatedAt",
)

rt_doc_revision = get_rt_xml_text(
    rt_doc_header,
    "DocRevision",
)


print("=== RT DOCUMENT METADATA ===\n")

print(f"DeliveryDate : {rt_delivery_date}")
print(f"DeliveryHour : {rt_delivery_hour}")
print(f"CreatedAt    : {rt_created_at}")
print(f"DocRevision  : {rt_doc_revision}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_interval_records = []


for zonal_price in rt_doc_body.findall(
    rt_tag("ZonalPrice")
):

    interval = get_rt_xml_text(
        zonal_price,
        "Interval",
    )

    flag = get_rt_xml_text(
        zonal_price,
        "Flag",
    )

    lmp_cap = get_rt_xml_text(
        zonal_price,
        "LmpCap",
    )

    loss_price_cap = get_rt_xml_text(
        zonal_price,
        "LossPriceCap",
    )

    congestion_price_cap = get_rt_xml_text(
        zonal_price,
        "CongPriceCap",
    )

    rt_interval_records.append(
        {
            "DeliveryDate": rt_delivery_date,
            "DeliveryHour": (
                int(rt_delivery_hour)
                if rt_delivery_hour is not None
                else None
            ),
            "Interval": (
                int(interval)
                if interval is not None
                else None
            ),
            "ZonalPriceCapped": (
                parse_rt_decimal(lmp_cap)
            ),
            "LossPriceCapped": (
                parse_rt_decimal(loss_price_cap)
            ),
            "CongestionPriceCapped": (
                parse_rt_decimal(
                    congestion_price_cap
                )
            ),
            "Flag": flag,
            "SourceCreatedAt": rt_created_at,
            "SourceDocRevision": rt_doc_revision,
        }
    )


rt_interval_df = pd.DataFrame(
    rt_interval_records
)


print(
    f"Flattened RT interval slots: "
    f"{len(rt_interval_df):,}"
)

display(rt_interval_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RT_GRAIN = [
    "DeliveryDate",
    "DeliveryHour",
    "Interval",
]


rt_duplicate_mask = (
    rt_interval_df.duplicated(
        subset=RT_GRAIN,
        keep=False,
    )
)

rt_duplicate_rows = (
    rt_interval_df[
        rt_duplicate_mask
    ]
)


print("=== RT CANDIDATE GRAIN ===\n")

print(
    f"Total interval slots : "
    f"{len(rt_interval_df):,}"
)

print(
    f"Unique keys          : "
    f"{rt_interval_df[RT_GRAIN].drop_duplicates().shape[0]:,}"
)

print(
    f"Duplicate rows       : "
    f"{len(rt_duplicate_rows):,}"
)


if rt_duplicate_rows.empty:
    print(
        "\nCandidate grain validation: PASS"
    )
else:
    print(
        "\nCandidate grain validation: INVESTIGATE"
    )

    display(rt_duplicate_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

observed_intervals = set(
    rt_interval_df[
        "Interval"
    ].dropna()
)

expected_intervals = set(
    range(1, 13)
)


print("=== RT INTERVAL COVERAGE ===\n")

print(
    f"Observed   : "
    f"{sorted(observed_intervals)}"
)

print(
    f"Missing    : "
    f"{sorted(expected_intervals - observed_intervals)}"
)

print(
    f"Unexpected : "
    f"{sorted(observed_intervals - expected_intervals)}"
)


assert (
    observed_intervals
    == expected_intervals
), (
    "RT report does not expose the expected "
    "12 five-minute interval slots."
)

print(
    "\nRT interval coverage validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PRICE_COLUMNS_RT = [
    "ZonalPriceCapped",
    "LossPriceCapped",
    "CongestionPriceCapped",
]


rt_interval_df["populated_price_count"] = (
    rt_interval_df[
        PRICE_COLUMNS_RT
    ]
    .notna()
    .sum(axis=1)
)

rt_interval_df["is_fully_populated"] = (
    rt_interval_df[
        PRICE_COLUMNS_RT
    ]
    .notna()
    .all(axis=1)
)

rt_interval_df["is_fully_empty"] = (
    rt_interval_df[
        PRICE_COLUMNS_RT
    ]
    .isna()
    .all(axis=1)
)


display(
    rt_interval_df[
        [
            "DeliveryDate",
            "DeliveryHour",
            "Interval",
            "ZonalPriceCapped",
            "LossPriceCapped",
            "CongestionPriceCapped",
            "Flag",
            "populated_price_count",
            "is_fully_populated",
            "is_fully_empty",
        ]
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== RT SLOT COMPLETENESS ===\n")

print(
    "Fully populated :",
    int(
        rt_interval_df[
            "is_fully_populated"
        ].sum()
    ),
)

print(
    "Fully empty     :",
    int(
        rt_interval_df[
            "is_fully_empty"
        ].sum()
    ),
)

partial_slots = rt_interval_df[
    ~rt_interval_df[
        "is_fully_populated"
    ]
    &
    ~rt_interval_df[
        "is_fully_empty"
    ]
]

print(
    "Partially populated:",
    len(partial_slots),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

average_price_element = rt_doc_body.find(
    rt_tag("AveragePrice")
)

assert average_price_element is not None


rt_average_price = {
    "ZonalPriceCapped": parse_rt_decimal(
        get_rt_xml_text(
            average_price_element,
            "LmpCap",
        )
    ),
    "LossPriceCapped": parse_rt_decimal(
        get_rt_xml_text(
            average_price_element,
            "LossPriceCap",
        )
    ),
    "CongestionPriceCapped": parse_rt_decimal(
        get_rt_xml_text(
            average_price_element,
            "CongPriceCap",
        )
    ),
}


print("=== RT AVERAGE PRICE ===\n")

for key, value in rt_average_price.items():
    print(
        f"{key:<25}: {value}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

published_rt_df = (
    rt_interval_df[
        rt_interval_df[
            "is_fully_populated"
        ]
    ]
    .copy()
)


calculated_average = {}


for column in PRICE_COLUMNS_RT:

    values = published_rt_df[
        column
    ].tolist()

    calculated_average[
        column
    ] = (
        sum(values)
        / Decimal(len(values))
        if values
        else None
    )


print(
    "=== RT AVERAGE PRICE INVESTIGATION ===\n"
)

for column in PRICE_COLUMNS_RT:

    reported = (
        rt_average_price[column]
    )

    calculated = (
        calculated_average[column]
    )

    difference = (
        reported - calculated
        if (
            reported is not None
            and calculated is not None
        )
        else None
    )

    print(f"{column}")
    print(
        f"  Reported   : {reported}"
    )
    print(
        f"  Calculated : {calculated}"
    )
    print(
        f"  Difference : {difference}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert (
    rt_interval_df[
        "DeliveryHour"
    ]
    .between(
        1,
        24,
    )
    .all()
), (
    "DeliveryHour is outside the documented 1–24 domain."
)

print(
    "RT DeliveryHour validation: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RT_PRICE_CONFIG = SourceDiscoveryConfig(
    source_name="ieso_realtime_ontario_zonal_price",

    required_header_columns=(),

    expected_columns=(
        "DeliveryDate",
        "DeliveryHour",
        "Interval",
        "ZonalPriceCapped",
        "LossPriceCapped",
        "CongestionPriceCapped",
        "Flag",
        "SourceCreatedAt",
        "SourceDocRevision",
    ),

    candidate_grain=(
        "DeliveryDate",
        "DeliveryHour",
        "Interval",
    ),

    date_column="DeliveryDate",

    numeric_columns=(
        "DeliveryHour",
        "Interval",
        "ZonalPriceCapped",
        "LossPriceCapped",
        "CongestionPriceCapped",
    ),

    observed_rows_per_complete_day=None,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rt_profile_df = (
    rt_interval_df[
        [
            "DeliveryDate",
            "DeliveryHour",
            "Interval",
            "ZonalPriceCapped",
            "LossPriceCapped",
            "CongestionPriceCapped",
            "Flag",
            "SourceCreatedAt",
            "SourceDocRevision",
        ]
    ]
    .copy()
)


rt_profile = build_source_profile(
    dataframe=rt_profile_df,
    config=RT_PRICE_CONFIG,
    header_row_index=-1,
)

print_source_profile(
    rt_profile
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert rt_profile[
    "duplicate_key_count"
] == 0

assert rt_profile[
    "invalid_date_count"
] == 0

assert len(rt_profile_df) == 12

assert set(
    rt_profile_df["Interval"]
) == set(range(1, 13))

discovery_results[
    RT_PRICE_CONFIG.source_name
] = rt_profile


print(
    "Registered discovery profiles:"
)

for source_name in discovery_results:
    print(f" - {source_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DA_PRICE_CONFIG = SourceDiscoveryConfig(
    source_name="ieso_day_ahead_ontario_zonal_price",

    required_header_columns=(),

    expected_columns=(
        "DeliveryDate",
        "PricingHour",
        "ZonalPrice",
        "LossPriceCapped",
        "CongestionPriceCapped",
        "Flag",
    ),

    candidate_grain=(
        "DeliveryDate",
        "PricingHour",
    ),

    date_column="DeliveryDate",

    numeric_columns=(
        "PricingHour",
        "ZonalPrice",
        "LossPriceCapped",
        "CongestionPriceCapped",
    ),

    observed_rows_per_complete_day=None,
)


da_profile = build_source_profile(
    dataframe=da_price_df,
    config=DA_PRICE_CONFIG,
    header_row_index=-1,
)


assert da_profile[
    "duplicate_key_count"
] == 0

assert da_profile[
    "invalid_date_count"
] == 0


discovery_results[
    DA_PRICE_CONFIG.source_name
] = da_profile

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "=== GRIDPULSE DISCOVERY REGISTRY ===\n"
)

for index, source_name in enumerate(
    discovery_results,
    start=1,
):
    print(
        f"{index}. {source_name}"
    )

assert len(discovery_results) == 5

print(
    "\nFive-source discovery registry: PASS"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Discovery findings — Real-Time Ontario Zonal Price
# 
# - Source format: XML.
# - The source is published every five minutes for the current dispatch hour.
# - The report exposes 12 five-minute interval slots.
# - Observed candidate grain: `DeliveryDate + DeliveryHour + Interval`.
# - All 12 candidate keys were unique in the inspected snapshot.
# - Price components are contractually nullable through the `empty_or_decimal` XSD type.
# - The inspected snapshot contained 9 fully populated interval slots and 3 fully empty future/unavailable slots.
# - Empty interval slots are preserved in Bronze and are not treated as malformed records.
# - `LmpCap` is mapped semantically to the capped Ontario Zonal Price in downstream layers.
# - `LossPriceCap` and `CongPriceCap` are preserved as the capped loss and congestion components.
# - `Flag` is preserved as source metadata.
# - `AveragePrice` is stored separately from five-minute interval records and is not treated as an additional market interval.
# - In the inspected snapshot, reported `AveragePrice` values matched the arithmetic mean of populated interval values rounded to two decimals; this remains an observed behaviour rather than a confirmed contractual formula.
# - The mutable latest-source alias requires payload-hash and source-created-time awareness.
# - Event publication should distinguish the business interval key from the source/event revision.

