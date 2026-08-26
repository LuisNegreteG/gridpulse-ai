# GridPulse AI — Naming Conventions

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform  
**Version:** 1.0  
**Last reviewed:** 2026-08-25

---

## 1. Purpose

This document defines naming conventions used across GridPulse AI.

The objective is to keep Microsoft Fabric artifacts, Lakehouse objects, source files, code, operational metadata, Git assets, and documentation consistent and easy to navigate.

Naming should favor:

- clarity;
- predictability;
- searchability;
- business meaning;
- compatibility across Fabric, Spark, SQL, KQL, Python, and Git.

Avoid abbreviations unless they are widely understood within the project.

---

# 2. General Rules

## 2.1 Case Style

Use:

```text
lower_snake_case
```

for:

- tables;
- columns;
- schemas;
- Python variables where practical;
- file/folder names;
- operational fields.

Examples:

```text
market_date
hour_ending
ontario_demand_mw
source_created_at
```

Do not mix styles such as:

```text
MarketDate
marketDate
market-date
MARKET_DATE
```

inside analytical datasets unless required by an external system.

---

## 2.2 Names Should Express Meaning

Prefer:

```text
ontario_demand_mw
```

over:

```text
value
demand1
metric
```

Prefer:

```text
zonal_price_cad_per_mwh
```

over:

```text
price
```

when the additional context materially improves meaning.

---

## 2.3 Units

Include units in measure names when useful.

Examples:

```text
market_demand_mw
ontario_demand_mw
output_mwh
zonal_price_cad_per_mwh
```

Do not append units to categorical or key fields.

---

# 3. Fabric Workspace Naming

Pattern:

```text
ws-<project>-<environment>
```

Current workspace:

```text
ws-gridpulse-dev
```

Potential future environments:

```text
ws-gridpulse-test
ws-gridpulse-prod
```

Do not create additional environments unless the deployment strategy requires them.

---

# 4. Lakehouse Naming

Pattern:

```text
lh_<project>
```

Current Lakehouse:

```text
lh_gridpulse
```

If future physical separation becomes necessary, names may use:

```text
lh_gridpulse_bronze
lh_gridpulse_silver
lh_gridpulse_gold
```

but this is not part of the current MVP architecture.

---

# 5. Lakehouse Schema Naming

Current schemas:

```text
silver
gold
ops
```

Purpose:

```text
silver
→ normalized trusted datasets

gold
→ business-oriented analytical datasets

ops
→ operational metadata and DataOps evidence
```

Bronze uses OneLake Files rather than a table schema:

```text
Files/bronze/
```

---

# 6. Table Naming

Tables use:

```text
lower_snake_case
```

No unnecessary prefixes such as:

```text
tbl_
table_
data_
```

unless a platform requirement explicitly requires them.

---

## 6.1 Silver Tables

Pattern:

```text
silver.<business_entity>_<grain_or_context>
```

Current planned tables:

```text
silver.demand_hourly
silver.demand_zonal_hourly
silver.generation_hourly
silver.price_day_ahead_hourly
silver.price_realtime_5min
```

Names describe the dataset rather than the physical source filename.

---

## 6.2 Gold Fact Tables

Pattern:

```text
gold.fact_<business_subject>_<grain>
```

Current planned facts:

```text
gold.fact_market_hourly
gold.fact_generation_hourly
gold.fact_zonal_demand_hourly
gold.fact_realtime_price_5min
```

`fact_` is used intentionally in Gold because these datasets represent analytical facts.

---

## 6.3 Dimension Tables

If dimensions become necessary:

```text
gold.dim_<business_entity>
```

Examples:

```text
gold.dim_date
gold.dim_zone
gold.dim_fuel
```

Dimensions must only be introduced when they provide real analytical or governance value.

---

## 6.4 Operational Tables

Pattern:

```text
ops.<operational_subject>
```

Planned:

```text
ops.etl_run
ops.source_file_registry
ops.dq_result
```

Avoid mixing operational metadata into analytical Gold tables.

---

# 7. Column Naming

Columns use:

```text
lower_snake_case
```

---

## 7.1 Business Keys

Prefer meaningful business-key names:

```text
market_date
hour_ending
zone
fuel_type
delivery_date
delivery_hour
interval
```

Avoid generic names such as:

```text
date1
key1
id2
```

---

## 7.2 Time Fields

Use names that distinguish market time from system-processing time.

Examples:

```text
market_date
delivery_date
hour_ending
delivery_hour
interval

source_created_at
ingestion_timestamp
start_timestamp
end_timestamp
```

Do not call all timestamps:

```text
timestamp
```

because their semantics differ.

---

## 7.3 Boolean Fields

Use affirmative predicates such as:

```text
is_valid
is_complete
is_current
is_fully_populated
```

Avoid ambiguous fields such as:

```text
flag1
status_bool
check
```

---

## 7.4 Count Fields

Use:

```text
records_read
records_written
records_rejected
duplicate_count
failed_record_count
```

---

# 8. Technical Metadata Naming

GridPulse-owned lineage fields use a leading underscore when stored alongside business data.

Examples:

```text
_source_name
_source_file
_source_url
_source_hash
_source_version
_source_created_at
_source_doc_revision
_ingestion_timestamp
_run_id
```

The underscore communicates:

```text
technical metadata
≠
business measure
```

Operational tables such as `ops.etl_run` do not require the underscore convention because the entire table is technical metadata.

---

# 9. Bronze Directory Naming

Pattern:

```text
Files/bronze/<provider>/<dataset>/
```

Current structure:

```text
Files/bronze/ieso/demand/
Files/bronze/ieso/demand_zonal/
Files/bronze/ieso/generation/
Files/bronze/ieso/price_day_ahead/
Files/bronze/ieso/price_realtime/
```

Directory names represent logical source domains rather than copying long report names.

---

# 10. Bronze File Naming

Source filenames should be preserved where they provide stable source identity.

When GridPulse creates immutable snapshots, additional metadata may be appended.

Example:

```text
PUB_Demand_2026
__sha256_91c336a83a84.csv
```

Real-Time mutable aliases use:

```text
PUB_RealtimeOntarioZonalPrice
__retrieved_YYYYMMDDTHHMMSSZ
__sha256_<short_hash>.xml
```

The full SHA-256 remains stored in operational metadata even when a shortened hash is used in the physical filename.

---

# 11. Notebook Naming

Pattern:

```text
nb_<sequence>_<purpose>
```

Current:

```text
nb_01_source_discovery
```

Future examples:

```text
nb_02_bronze_ingestion
nb_03_silver_transformations
nb_04_data_quality
nb_05_gold_model
```

Sequence numbers communicate intended workflow order.

They do not imply that notebooks must become one large monolithic pipeline.

---

# 12. Pipeline Naming

Pattern:

```text
pl_<purpose>
```

Examples:

```text
pl_ieso_batch_ingestion
pl_gridpulse_daily_refresh
pl_silver_processing
```

If source-specific orchestration becomes necessary:

```text
pl_ieso_demand_ingestion
```

Prefer reusable orchestration before creating one pipeline for every file.

---

# 13. Real-Time Intelligence Naming

## Eventstream

Pattern:

```text
es_<subject>
```

Planned:

```text
es_realtime_price
```

---

## Eventhouse

Pattern:

```text
eh_<project_or_domain>
```

Planned:

```text
eh_gridpulse
```

---

## KQL Database

Pattern:

```text
kqldb_<domain>
```

Potential:

```text
kqldb_market_realtime
```

Final names will be validated against Fabric item requirements during implementation.

---

## KQL Querysets

Pattern:

```text
kql_<purpose>
```

Examples:

```text
kql_realtime_price_monitoring
kql_market_investigation
```

---

# 14. Source Identifiers

Human-readable source IDs:

```text
SRC-001
SRC-002
SRC-003
SRC-004
SRC-005
```

Logical source names:

```text
ieso_hourly_demand
ieso_hourly_zonal_demand
ieso_generation_by_fuel_hourly
ieso_day_ahead_ontario_zonal_price
ieso_realtime_ontario_zonal_price
```

The logical source name should remain stable even if the physical source filename changes.

---

# 15. Data Quality Rule Naming

Pattern:

```text
<DOMAIN>-<NNN>
```

Examples:

```text
DEMAND-001
DEMAND-002

ZONAL-001
ZONAL-002

GEN-001
GEN-002

DA-001
DA-002

RT-001
RT-002
```

Rule identifiers must remain stable after implementation because they may be referenced by:

```text
ops.dq_result
documentation
tests
alerts
runbooks
```

If a rule is retired, its identifier should not be reused for a different meaning.

---

# 16. Architecture Decision Naming

Pattern:

```text
ADR-<NNN>
```

Examples:

```text
ADR-001
ADR-002
ADR-007
```

The identifier remains stable even when an ADR becomes:

```text
SUPERSEDED
```

---

# 17. Run Identifiers

`run_id` should be unique per logical pipeline execution.

The final implementation may use:

```text
UUID
```

or an equivalent globally unique identifier.

Human-readable timestamps may supplement but should not replace unique run identity.

---

# 18. Python Naming

Follow standard Python conventions.

Functions:

```text
snake_case
```

Examples:

```python
parse_csv_source()
build_source_profile()
calculate_payload_hash()
```

Classes:

```text
PascalCase
```

Example:

```python
SourceDiscoveryConfig
```

Constants:

```text
UPPER_SNAKE_CASE
```

Examples:

```python
DA_XML_NS
RT_XML_NS
RT_GRAIN
```

---

# 19. Repository Naming

Repository:

```text
gridpulse-ai
```

Directories use:

```text
lower_snake_case
```

Current/target structure:

```text
architecture/
docs/
notebooks/
ingestion/
realtime/
sql/
agents/
evaluation/
tests/
fabric/
```

---

# 20. Git Branch Naming

Permanent branches:

```text
main
develop
```

Feature branches, when useful:

```text
feature/<short-description>
```

Examples:

```text
feature/silver-transformations
feature/realtime-eventstream
feature/data-quality-framework
```

Bug fixes:

```text
fix/<short-description>
```

Example:

```text
fix/rt-revision-deduplication
```

Avoid branches such as:

```text
test123
new
final
luis-branch
```

---

# 21. Git Commit Naming

Commit messages should describe the engineering change.

Preferred pattern:

```text
<type>: <concise change>
```

Useful types:

```text
feat
fix
docs
test
refactor
chore
```

Examples:

```text
docs: add IESO source catalog
docs: define initial data contracts
docs: record architecture decisions
feat: implement hourly demand ingestion
feat: add silver zonal demand transformation
test: add source parser unit tests
fix: handle revised real-time price payloads
```

Avoid:

```text
changes
update
final version
stuff
working now
```

---

# 22. Environment Naming

Environment suffixes:

```text
dev
test
prod
```

Only create environments that actually exist.

Do not label an artifact:

```text
prod
```

unless it is genuinely used as a production environment.

The current project environment is:

```text
dev
```

---

# 23. Naming Change Policy

Names that become external contracts should not be casually changed.

Particularly stable names include:

```text
table names
business-key columns
DQ rule IDs
source identifiers
tool names
```

Before renaming a stable object, evaluate:

```text
downstream impact
tests
documentation
pipelines
SQL
KQL
AI tools
```

Cosmetic renames are not worth breaking an established contract.

---

# 24. Guiding Principle

A GridPulse name should allow another engineer to infer:

```text
what the object represents
what layer it belongs to
what grain or purpose it has
```

without opening the implementation first.

Consistency is preferred over cleverness.
