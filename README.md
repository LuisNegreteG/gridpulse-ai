# GridPulse AI

**Ontario Real-Time Energy Intelligence & DataOps Platform**

GridPulse AI is a production-oriented data engineering portfolio project built around public electricity-market data from Ontario's **Independent Electricity System Operator (IESO)**.

The project is designed to demonstrate how an enterprise data platform can move from **business requirements → governed ingestion → trusted analytical data → real-time intelligence → grounded AI-assisted investigation**.

Rather than focusing on a single dashboard or notebook, GridPulse AI emphasizes the engineering concerns required to operate a reliable data platform:

* explicit data grain;
* immutable raw-data preservation;
* source revision detection;
* incremental and idempotent ingestion;
* reusable data-quality controls;
* observability and lineage;
* dimensional modeling;
* batch and near-real-time processing;
* testing and failure handling;
* architecture decision records;
* CI/CD considerations;
* AI grounded in governed data.

---

## Business Scenario

GridPulse Energy Analytics is a fictional analytics company providing intelligence about Ontario's electricity market.

The primary user is an **Energy Operations Manager**, supported by analysts and operational leadership.

The platform is designed to answer:

> **What happened in Ontario's electricity market, when did it happen, and what data explains it?**

Representative analytical questions include:

* What was Ontario's peak electricity demand for a selected day?
* At what hour did peak demand occur?
* How did demand differ across Ontario zones?
* What was the generation mix during peak demand?
* What was the Day-Ahead Ontario Zonal Price for each hour?
* How did Real-Time price differ from Day-Ahead price?
* When were DA-vs-RT price deviations largest?
* What were demand and generation doing during unusual price movements?
* Is the underlying data sufficiently complete and fresh to trust the analysis?

---

## Architecture

```text
                         IESO PUBLIC MARKET REPORTS
                                   |
                 +-----------------+-----------------+
                 |                                   |
                 |                                   |
            BATCH PATH                       NEAR-REAL-TIME PATH
                 |                                   |
                 v                                   v
       Fabric Data Engineering               Python RT Publisher
                 |                                   |
                 v                                   v
              OneLake                          Fabric Eventstream
                 |                                   |
                 v                                   v
          Bronze Raw Files                       Eventhouse
                 |                                   |
                 v                                   v
              PySpark                                KQL
                 |
                 v
        Data Quality Controls
                 |
                 v
              Silver
                 |
                 v
               Gold
                 |
                 v
       SQL Analytics Endpoint
                 |
                 +-------------------+
                                     |
                                     v
                         AI Investigation Layer
                           /       |        \
                          /        |         \
                    SQL tools   KQL tools   Metadata/DQ
```

---

## Medallion Architecture

### Bronze

Raw source payloads are preserved without business transformation.

```text
Files/
└── bronze/
    └── ieso/
        ├── demand/
        ├── demand_zonal/
        ├── generation/
        ├── price_day_ahead/
        └── price_realtime/
```

Bronze is designed to support:

* reproducibility;
* source lineage;
* reprocessing;
* revision detection;
* forensic investigation.

Payload identity is based on more than filename alone. SHA-256 hashes and source-version metadata are used to distinguish unchanged files from revised source publications.

### Silver

Planned normalized Delta tables:

```text
silver.demand_hourly
silver.demand_zonal_hourly
silver.generation_hourly
silver.price_day_ahead_hourly
silver.price_realtime_5min
```

Silver responsibilities include:

* schema enforcement;
* parsing and normalization;
* type conversion;
* grain enforcement;
* duplicate handling;
* source-quality preservation;
* technical metadata;
* quarantine where appropriate.

### Gold

Gold intentionally avoids combining incompatible grains into one oversized table.

Planned analytical tables include:

```text
gold.fact_market_hourly
gold.fact_generation_hourly
gold.fact_zonal_demand_hourly
gold.fact_realtime_price_5min
```

Supporting dimensions will only be introduced when they provide analytical value.

---

## Data Sources

GridPulse uses public data published by IESO.

### 1. Hourly Demand

Contains hourly:

* Market Demand
* Ontario Demand

Observed source grain:

```text
Date + Hour
```

### 2. Hourly Zonal Demand

Contains Ontario demand together with regional demand across zones such as:

* Northwest
* Northeast
* Ottawa
* East
* Toronto
* Essa
* Bruce
* Southwest
* Niagara
* West

The physical source is wide:

```text
Date + Hour + multiple zone columns
```

Silver will normalize it to:

```text
market_date + hour + zone
```

### 3. Generator Output by Fuel Type Hourly

XML source containing IESO-reported hourly generation categories.

Observed flattened grain:

```text
Date + Hour + Fuel
```

Observed categories during source discovery included:

```text
BIOFUEL
CONTROL ACTIONS
GAS
HYDRO
NUCLEAR
OTHER
SOLAR
WIND
```

Source-provided quality information is retained rather than silently discarded.

### 4. Day-Ahead Ontario Zonal Price

Daily XML reports containing hourly Day-Ahead Ontario Zonal Price information.

Observed grain:

```text
DeliveryDate + PricingHour
```

Relevant source fields include:

```text
ZonalPrice
LossPriceCapped
CongestionPriceCapped
Flag
```

### 5. Real-Time Ontario Zonal Price

Five-minute real-time price source used by the near-real-time architecture.

Observed grain:

```text
DeliveryDate + DeliveryHour + Interval
```

The source exposes twelve five-minute interval slots for a dispatch hour.

The mutable current-report alias requires snapshot and revision awareness rather than filename-only deduplication.

---

## Source Discovery

The first engineering phase profiles each source before production contracts are defined.

The discovery framework captures:

* source URL;
* source format;
* filename;
* file size;
* SHA-256 hash;
* source metadata;
* schema;
* row count;
* date coverage;
* null counts;
* duplicate analysis;
* candidate grain;
* sample records;
* source-specific quirks.

### Current discovery status

```text
ieso_hourly_demand                    PASS
ieso_hourly_zonal_demand              PASS
ieso_generation_by_fuel_hourly        PASS
ieso_day_ahead_ontario_zonal_price    PASS
ieso_realtime_ontario_zonal_price     PASS
```

All five MVP sources have completed initial structural discovery.

---

## Selected Discovery Findings

Source discovery has already identified behaviours that directly influence platform design.

### Source revisions are real

IESO publishes versioned/revised reports.

GridPulse therefore distinguishes:

```text
same filename + same hash
→ unchanged payload

same filename + different hash
→ revised source payload
```

### Latest date does not imply completeness

A source may expose the latest business date while only a subset of expected hourly or interval data has been published.

Freshness therefore requires more than:

```text
MAX(date)
```

### Cross-source reconciliation matters

Ontario Demand was reconciled between the Hourly Demand and Hourly Zonal Demand reports.

The inspected overlapping dataset contained one source-level discrepancy.

GridPulse preserves both values and records the reconciliation issue instead of silently overwriting either source.

### Source nulls are not automatically bad data

The Generation XML schema permits `Output` to be absent.

Similarly, Day-Ahead and Real-Time price schemas allow empty monetary values.

Structural validity, business completeness and semantic quality are therefore treated as separate concepts.

### Real-time slots and published events are different concepts

The Real-Time report can expose all twelve interval slots while future intervals remain empty.

GridPulse preserves the complete source snapshot but will only publish sufficiently populated market events into the streaming path.

---

## Data Quality Strategy

GridPulse is designed around reusable data-quality controls.

Planned categories include:

```text
Schema
Completeness
Uniqueness
Grain
Validity
Freshness
Referential consistency
Cross-source reconciliation
Volume anomalies
```

Potential outcomes include:

```text
PASS
WARN
FAIL
```

Invalid data must not disappear silently.

Conceptually:

```text
valid record
→ trusted Silver

structurally invalid record
→ quarantine

valid but suspicious record
→ preserve + DQ finding
```

Business rules are not created without evidence from the source or authoritative documentation.

---

## Observability

The operational layer is designed around tables such as:

```text
ops.etl_run
ops.source_file_registry
ops.dq_result
```

### Source file registry

Tracks information such as:

```text
source_name
file_name
source_url
file_size
file_hash
source_version
first_seen_ts
last_seen_ts
processing_status
run_id
```

### ETL execution

Tracks:

```text
run_id
pipeline_name
source_name
start_ts
end_ts
status
records_read
records_written
records_rejected
error_message
```

This allows data correctness, freshness and execution health to be evaluated independently.

---

## Real-Time Intelligence

## Real-Time Intelligence

Phase 4 implements a revision-aware near-real-time processing path for the IESO Real-Time Ontario Zonal Price source.

```text
IESO Real-Time Ontario Zonal Price
                |
                v
     Single-poll Python Publisher
                |
                v
      Immutable Bronze Evidence
                |
                v
    Revision-Aware Event Logic
                |
                v
    Durable Delta Outbox
                |
                v
       Fabric Eventstream
                |
                v
          Eventhouse
                |
                v
        KQL Event History
                |
        +-------+-------+
        |               |
        v               v
   Current State   Rolling Metrics
        |
        v
   DQ / Observability
```

The publisher will distinguish:

### Business identity

```text
delivery_date
delivery_hour
interval
```

### Source revision identity

```text
business key
+ source_created_at
+ source_hash
```

This allows revised market observations to be retained rather than silently discarded.

---

## AI Investigation Layer

The final platform will include an AI-assisted investigation layer grounded in actual platform data.

The AI layer will use controlled tools such as:

```text
get_demand
get_zonal_demand
get_generation_mix
get_day_ahead_price
get_realtime_price
compare_da_vs_rt_price
get_peak_demand
get_market_summary
investigate_market_event
get_data_quality_status
```

Critical KPIs are calculated by tools rather than freely generated by the language model.

If sufficient evidence does not exist, the expected behaviour is:

> **I don't have sufficient data to answer this question.**

The agent will later be evaluated for:

* tool-selection accuracy;
* parameter correctness;
* numerical grounding;
* unsupported claims;
* failure behaviour;
* latency.

No agent-quality metric will be reported before it has actually been measured.

---

## Testing Strategy

GridPulse will use three testing layers.

### Unit Tests

Examples:

* XML parsers;
* reusable transformation functions;
* schema validators;
* hashing and revision helpers.

### Data Tests

Examples:

* grain uniqueness;
* completeness;
* schema compliance;
* date continuity;
* referential integrity;
* source reconciliation.

### Agent Tests

Examples:

* expected tool selection;
* parameter correctness;
* grounded numerical response;
* unsupported-question handling.

Failure scenarios will include:

```text
source unavailable
empty file
schema change
duplicate source
revised source
bad timestamp
incomplete data
unsupported AI question
```

---

## Architecture Principles

GridPulse follows several explicit engineering principles:

1. Business-driven architecture
2. Explicit data grain
3. Immutable raw-data preservation
4. Idempotent ingestion
5. Incremental processing
6. Source revision awareness
7. Reusable data-quality controls
8. Observability
9. Data lineage
10. Separation of analytical grains
11. Batch + near-real-time processing
12. Testing and failure handling
13. Security by design
14. CI/CD awareness
15. Architecture Decision Records
16. Grounded AI analytics
17. Measurable rather than claimed scalability

---

## Technology Stack

The implementation is centered on:

```text
Microsoft Fabric
OneLake
Fabric Lakehouse
Delta Lake
PySpark / Spark
Python
SQL
Fabric Data Factory
Fabric Eventstream
Fabric Eventhouse
KQL
Power BI
Git / GitHub
```

Technologies are introduced only when they solve a specific architectural requirement.

---

## Repository Structure

```text
gridpulse-ai/
│
├── README.md
│
├── architecture/
│
├── docs/
│   ├── business_requirements.md
│   ├── source_catalog.md
│   ├── data_contracts.md
│   ├── architecture_decisions.md
│   ├── data_quality.md
│   ├── security.md
│   └── runbook.md
│
├── notebooks/
├── ingestion/
├── realtime/
├── sql/
├── agents/
├── evaluation/
├── tests/
└── fabric/
```

Fabric-managed workspace definitions will later be stored separately from manually maintained documentation and supporting code.

---

## Development Roadmap

### Phase 1 — Architecture & Source Discovery

* [x] Fabric development workspace
* [x] Schema-enabled Lakehouse
* [x] Bronze landing zones
* [x] Source discovery notebook
* [x] Five IESO source investigations
* [x] Source grain validation
* [x] Source revision discovery
* [x] Source catalog
* [x] Initial data contracts
* [x] Architecture Decision Records
* [x] GitHub/Fabric integration

### Phase 2 — Bronze & Silver Engineering

* [x] Reusable ingestion framework
* [x] Incremental Bronze ingestion
* [x] Revision-aware source registry
* [x] PySpark transformations
* [x] Silver Delta tables
* [x] Quarantine handling
* [x] Technical lineage metadata

### Phase 3 — Gold, Quality & Orchestration

* [x] Gold analytical model
* [x] Reusable DQ framework
* [x] ETL run-control framework
* [x] Incremental processing
* [x] Idempotent MERGE patterns
* [x] Fabric orchestration

### Phase 4 — Real-Time Intelligence

* [x] Revision-aware Real-Time source publisher
* [x] Exact Bronze evidence reuse
* [x] Durable publisher checkpoint
* [x] Durable Delta event outbox
* [x] Retry-safe leased dispatcher
* [x] Fabric Eventstream
* [x] Eventhouse event history
* [x] KQL current-state serving
* [x] Duplicate/revision handling
* [x] Rolling 15/30/60-minute market metrics
* [x] Real-Time DQ and observability
* [x] End-to-end Real-Time validation

### Phase 5 — AI & Production Readiness

* [ ] SQL/KQL investigation tools
* [ ] Grounded market-investigation agent
* [ ] Data-quality-aware responses
* [ ] Agent evaluation dataset
* [ ] Unit/data/agent tests
* [ ] Failure testing
* [ ] Production-readiness review

---

## Current Project Status

- Phase 1 — Architecture & Source Discovery: Complete
- Phase 2 — Bronze & Silver Engineering: Complete
- Phase 3 — Gold & Serving Engineering: 
- Phase 4 — Real-Time Intelligence: Complete

Current Gold layer:

- five source-aligned Gold Delta facts
- lightweight SQL serving views
- reusable Gold contracts and DQ
- independent Gold execution tracking
- persistent DQ evidence
- idempotent MERGE processing
- Delta Change Data Feed incremental processing
- version-based processing watermarks
- revision-propagation testing
- failure-path testing
- trusted coverage/intersection validation
- Fabric Data Factory orchestration
- end-to-end Bronze → Silver → Gold validation

Current Real-Time layer:

- single-poll revision-aware IESO publisher
- exact immutable Bronze lineage
- deterministic observation and event identity
- durable Delta outbox and completion checkpoint
- leased retry-safe Eventstream dispatcher
- append-oriented Eventhouse event history
- logical event deduplication
- KQL current-state serving
- gap-aware rolling 15/30/60-minute market metrics
- Real-Time DQ rules and operational observability
- validated end-to-end source → Bronze → outbox → Eventstream → Eventhouse flow

---

## Data Governance

All current sources are public IESO market reports.

```text
Classification: PUBLIC
PII: NONE OBSERVED
```

Credentials, secrets, connection strings, access tokens and API keys must never be committed to this repository.

---

## Data Attribution

GridPulse AI uses publicly available electricity-market data published by the **Independent Electricity System Operator (IESO)**.

GridPulse AI is an independent portfolio project and is **not affiliated with, endorsed by, or operated by IESO**.

Official IESO data resources:

* IESO Power Data / Data Directory
* IESO Public Reports

Source-specific details and observed behaviours are documented in:

```text
docs/source_catalog.md
```

---

## Engineering Goal

The objective of GridPulse AI is not simply to demonstrate the use of Microsoft Fabric.

The project is intended to demonstrate the ability to reason through:

```text
requirements
→ architecture
→ ingestion
→ source governance
→ trusted data
→ observability
→ analytics
→ real-time intelligence
→ grounded AI
```

with engineering decisions that can be explained, tested and defended.

## Phase 2 — Bronze & Silver Engineering

Phase 2 implemented the production-oriented ingestion and Silver transformation framework.

### Bronze

- Revision-aware and idempotent ingestion
- SHA-256 payload fingerprinting
- Immutable content-addressed raw storage
- Source registry and ETL run tracking
- NEW / UNCHANGED / REVISED / RECOVER handling
- Binary post-write integrity verification
- Five IESO sources supported through a shared ingestion framework

### Silver

Implemented Delta tables:

- `silver.demand_hourly`
- `silver.demand_zonal_hourly`
- `silver.generation_hourly`
- `silver.price_day_ahead_hourly`
- `silver.price_realtime_5min`

Silver processing includes:

- source-specific parsing
- explicit business grains
- typed schemas
- technical lineage
- Delta MERGE idempotency
- source-permitted null preservation
- reconciliation observations
- persistent DQ results in `ops.dq_result`

Latest Day 2 validation:

- Demand: 5,713 rows
- Zonal Demand: 57,120 rows
- Generation: 40,008 rows
- Day-Ahead Price: 24 rows
- Real-Time Price: 12 rows
- DQ results: 38
- DQ ERROR: 0
- DQ FAIL: 0
- Cross-source Ontario Demand reconciliation: 5,712 records checked, 1 WARN mismatch.