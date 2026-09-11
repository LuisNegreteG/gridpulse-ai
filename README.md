# GridPulse AI

**Ontario Real-Time Energy Intelligence & DataOps Platform**

GridPulse AI is a production-oriented Microsoft Fabric portfolio project built on public electricity-market data from Ontario's Independent Electricity System Operator (IESO).

The project demonstrates an end-to-end data platform that moves from **source discovery and governed ingestion** to **trusted analytical serving, near-real-time intelligence, data-quality evidence, and grounded AI-assisted investigation**.

> **Project status: COMPLETE — portfolio MVP finalized through Phase 5.**

---

## What GridPulse Demonstrates

GridPulse was designed to show more than a dashboard or a collection of notebooks. It focuses on the engineering controls needed to make analytical and AI-assisted outputs trustworthy:

- explicit source and analytical grain;
- immutable Bronze evidence;
- source revision detection;
- incremental and idempotent ingestion;
- reusable PySpark transformation patterns;
- Silver and Gold Delta modeling;
- persistent data-quality evidence;
- operational run tracking and lineage;
- Fabric Data Factory orchestration;
- revision-aware near-real-time processing;
- Eventstream and Eventhouse integration;
- KQL current-state and rolling metrics;
- deterministic SQL/KQL investigation contracts;
- evidence-aware AI investigation guardrails;
- failure-path and regression testing;
- Git-based development and pull-request workflow.

---

## Business Scenario

GridPulse Energy Analytics is a fictional analytics platform for Ontario electricity-market operations.

The primary analytical question is:

> **What happened in Ontario's electricity market, when did it happen, and what evidence supports the explanation?**

Representative questions include:

- What was Ontario's peak electricity demand for a selected day?
- At what hour did peak demand occur?
- How did demand differ across Ontario zones?
- What was the generation mix at a selected hour?
- What were the Day-Ahead price components?
- What was the current Real-Time price state?
- How did Real-Time price differ from Day-Ahead price where comparable?
- Were rolling Real-Time windows complete before a metric was interpreted?
- Is the available evidence complete enough to support a conclusion?

---

## High-Level Architecture

```text
                         IESO PUBLIC MARKET REPORTS
                                   |
                 +-----------------+-----------------+
                 |                                   |
                 v                                   v
            BATCH PATH                       NEAR-REAL-TIME PATH
                 |                                   |
        Fabric Data Engineering               Python Publisher
                 |                                   |
              OneLake                           Eventstream
                 |                                   |
              Bronze                             Eventhouse
                 |                                   |
              Silver                                KQL
                 |                            Event History / Current State
               Gold                           Rolling Market Metrics / DQ
                 |                                   |
        SQL Analytics Endpoint                       |
                 +-------------------+---------------+
                                     |
                                     v
                         Evidence-Aware Investigation
                              SQL + KQL + DQ
```

---

## Data Sources

GridPulse uses five public IESO source families:

1. **Hourly Demand** — Ontario and Market demand by market date and hour ending.
2. **Hourly Zonal Demand** — demand by Ontario zone.
3. **Generator Output by Fuel Type** — hourly generation categories and source quality information.
4. **Day-Ahead Ontario Zonal Price** — hourly zonal, loss, and congestion components.
5. **Real-Time Ontario Zonal Price** — five-minute price intervals used by the near-real-time path.

Source discovery established grain, schema, date coverage, null behavior, duplicate behavior, and revision characteristics before production transformations were implemented.

---

## Batch Data Platform

### Bronze

Raw source payloads are retained as immutable evidence with source metadata and hashes. This supports reproducibility, reprocessing, forensic investigation, and revision detection.

### Silver

Silver applies schema enforcement, parsing, normalization, type conversion, grain validation, duplicate handling, source-quality preservation, and technical lineage.

### Gold

Gold exposes source-aligned analytical facts rather than forcing incompatible grains into one table. Key serving objects include:

```text
gold.fact_market_demand_hourly
gold.fact_zonal_demand_hourly
gold.fact_generation_hourly
gold.fact_day_ahead_price_hourly
gold.fact_realtime_price_5min
```

Operational evidence is persisted through objects such as:

```text
ops.etl_run
ops.source_file_registry
ops.pipeline_watermark
ops.dq_result
```

The batch implementation includes incremental processing, idempotent MERGE behavior, Delta Change Data Feed usage, version-based watermarks, revision propagation, and failure-path validation.

---

## Data Quality & Observability

GridPulse separates three concepts that are often incorrectly conflated:

1. **Execution health** — did the process run successfully?
2. **Data quality** — did the applicable rules pass, warn, or fail?
3. **Evidence sufficiency** — is enough usable data available to answer the requested question?

Batch DQ results are persisted and the latest logical result is determined per source, dataset, and rule. Real-Time DQ uses governed KQL rules over Eventhouse state.

Important platform behaviors:

- `NULL` is never silently converted to zero.
- Negative electricity-price components are preserved as valid observations unless a specific rule says otherwise.
- Warnings do not automatically block all analysis.
- A passing DQ rule does not imply the requested evidence exists.
- Scope-specific quality findings are not treated as globally contagious.

---

## Real-Time Intelligence

The near-real-time path implements revision-aware publication and serving for IESO Real-Time price observations:

```text
IESO RT Source
    |
Single-Poll Publisher
    |
Immutable Bronze Evidence
    |
Revision-Aware Event Logic
    |
Durable Delta Outbox
    |
Retry-Safe Dispatcher
    |
Fabric Eventstream
    |
Eventhouse
    |
KQL Event History
    |
+----------------------+----------------------+
|                      |                      |
Current State       Price Change         Rolling Metrics
                                          15 / 30 / 60 min
```

Key Real-Time behaviors include:

- deterministic observation and event identity;
- durable outbox and completion checkpoint;
- revision and invalidation semantics;
- logical event deduplication;
- current eligible state by native business key;
- gap-aware interval-over-interval price change;
- completeness-aware rolling 15/30/60-minute metrics;
- Real-Time data-quality and observability rules.

---

## Evidence-Aware Investigation Layer

Phase 5 adds governed investigation contracts over trusted SQL and KQL serving layers.

### SQL serving contracts

Source-controlled deployment definitions are available in:

```text
fabric/sql/phase5_investigation_serving.sql
```

The contracts cover:

- latest persisted Batch DQ state;
- demand lookup;
- peak demand;
- zonal demand;
- generation mix;
- Day-Ahead prices;
- Day-Ahead vs historical Real-Time comparison;
- Batch DQ status.

### Real-Time serving contracts

The investigation notebook consumes governed Eventhouse functions for:

- current Real-Time price state;
- revision-aware logical history;
- gap-safe price change;
- rolling market metrics;
- Real-Time DQ state.

### Evidence states

Responses are classified deterministically as:

```text
SUFFICIENT
PARTIAL
INSUFFICIENT
BLOCKED_BY_DQ
```

Precedence is intentionally conservative:

1. no primary evidence → `INSUFFICIENT`;
2. required metrics unusable → `INSUFFICIENT`;
3. applicable blocking DQ failure → `BLOCKED_BY_DQ`;
4. incomplete requested scope → `PARTIAL`;
5. otherwise → `SUFFICIENT`.

The reasoning layer is not given unrestricted SQL or KQL execution. Critical metrics are calculated deterministically by governed serving logic rather than generated by an LLM.

### Composite investigation

`nb_09_ai_investigation` demonstrates multi-engine investigation by combining:

- Real-Time price evidence;
- deterministic Real-Time metrics;
- applicable DQ evidence;
- available hourly demand context;
- explicit limitations and evidence-state reasoning.

Concurrent market conditions are not presented as causal explanations.

---

## Validation

The final Phase 5 regression suite completed successfully:

```text
FINAL PHASE 5 EVALUATION: 10/10 PASSED
```

Validated behaviors include:

- complete Batch evidence;
- partial Batch coverage;
- missing Batch evidence;
- existing Real-Time evidence;
- missing Real-Time evidence;
- composite graceful degradation;
- blocking-DQ policy;
- invalid Hour Ending rejection;
- invalid Real-Time interval rejection;
- invalid date-format rejection.

The Real-Time layer was also validated for current-state uniqueness, rolling-window completeness behavior, gap-safe price-change behavior, and missing-key handling.

---

## Microsoft Fabric Components

The implemented platform uses:

```text
Microsoft Fabric
OneLake
Fabric Lakehouse
Delta Lake
PySpark / Spark
Python
SQL Analytics Endpoint
Fabric Data Factory
Fabric Eventstream
Fabric Eventhouse
KQL
Git / GitHub
```

A Fabric Data Agent was evaluated during Phase 5 but was not deployed because the available Fabric Trial capacity did not support creation of the required Data Agent workload. The deterministic investigation contracts remain runtime-independent and can be exposed through a supported agent runtime later without redesigning the data layer.

---

## Repository Layout

```text
gridpulse-ai/
├── README.md
├── architecture/
├── docs/
├── fabric/
│   ├── sql/
│   │   └── phase5_investigation_serving.sql
│   └── workspace/
│       ├── nb_01_source_discovery.Notebook/
│       ├── ...
│       ├── nb_09_ai_investigation.Notebook/
│       ├── Tables_Creation.KQLQueryset/
│       ├── eh_gridpulse_realtime.Eventhouse/
│       └── other Fabric-managed artifacts
```

Fabric-managed workspace definitions are retained separately from manually maintained documentation and SQL deployment contracts.

---

## Development Roadmap — Complete

### Phase 1 — Architecture & Source Discovery ✅

- Fabric development workspace
- schema-enabled Lakehouse
- Bronze landing zones
- source discovery for five IESO sources
- source grain and revision discovery
- data contracts and ADRs
- GitHub/Fabric integration

### Phase 2 — Bronze & Silver Engineering ✅

- reusable ingestion framework
- incremental Bronze ingestion
- revision-aware source registry
- PySpark transformations
- Silver Delta tables
- quarantine handling
- lineage metadata

### Phase 3 — Gold, Quality & Orchestration ✅

- source-aligned Gold model
- reusable DQ framework
- ETL run-control framework
- incremental/idempotent processing
- Fabric orchestration
- failure-path validation

### Phase 4 — Real-Time Intelligence ✅

- revision-aware publisher
- immutable Bronze evidence reuse
- durable outbox and checkpoint
- retry-safe dispatcher
- Eventstream and Eventhouse
- KQL current state
- rolling 15/30/60-minute metrics
- Real-Time DQ and observability
- end-to-end validation

### Phase 5 — Evidence-Aware AI & Production Readiness ✅

- deterministic SQL investigation contracts
- governed KQL investigation sources
- Batch and Real-Time DQ integration
- evidence-state evaluator
- evidence-aware demand and Real-Time adapters
- composite market-event investigation
- validation and failure tests
- final 10/10 regression suite
- source-controlled deployment artifacts
- production-readiness review

---

## Portfolio Summary

GridPulse demonstrates the design and implementation of a Microsoft Fabric data platform spanning batch and near-real-time workloads, with explicit lineage, revision handling, data-quality evidence, deterministic SQL/KQL serving contracts, and an evidence-aware investigation layer designed to prevent unsupported analytical claims.

The project is intentionally closed at the portfolio-MVP boundary. Future extensions may add a supported external or Fabric-native agent runtime, but the underlying deterministic data contracts and evidence model are complete.

---

## Data Governance

All current project sources are public IESO market reports.

```text
Classification: PUBLIC
PII: NONE OBSERVED
```

Secrets and credentials are not intended to be committed to the repository.

---

## Author

**Luis Carlo Negrete Girano**  
Data Engineering · Analytics Engineering · Microsoft Fabric · PySpark · SQL · KQL
