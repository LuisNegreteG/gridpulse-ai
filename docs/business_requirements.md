# GridPulse AI — Business Requirements

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform  
**Version:** 1.0  
**Status:** Approved for MVP implementation  
**Last reviewed:** 2026-08-25

---

## 1. Business Scenario

GridPulse Energy Analytics is a fictional analytics company that provides intelligence about Ontario's electricity market using publicly available market data published by the Independent Electricity System Operator (IESO).

The platform is designed to help operational and analytical users understand:

> **What happened in Ontario's electricity market, when did it happen, and what data explains it?**

GridPulse AI is not intended to reproduce every IESO reporting capability.

The MVP focuses on integrating a small set of complementary datasets into a trusted analytical and near-real-time data platform.

---

## 2. Primary User

### Energy Operations Manager

The primary user requires a reliable view of:

- Ontario electricity demand;
- regional demand;
- generation mix;
- Day-Ahead prices;
- Real-Time prices;
- unusual market movements;
- underlying data completeness and freshness.

The user must be able to investigate a market event without manually reconciling multiple raw reports.

---

## 3. Secondary Users

### Energy Analyst

Requires detailed market observations for analytical investigation.

### Data Analyst

Requires trusted and documented analytical datasets with clear grain and lineage.

### Operations Executive

Requires concise and trustworthy explanations of significant market conditions without needing to understand raw source structures.

---

## 4. Business Problem

IESO publishes multiple public electricity-market reports with different:

- formats;
- structures;
- grains;
- publication frequencies;
- revision behaviours;
- historical coverage;
- quality characteristics.

Using these reports independently makes it difficult to consistently answer cross-domain questions such as:

```text
What was happening with demand and generation
when Real-Time prices moved significantly?
```

The platform must therefore transform separate source reports into governed datasets that can be analyzed together without losing their native meaning.

---

## 5. Business Objective

GridPulse AI will provide a trusted data foundation capable of supporting:

```text
market observation
→ contextual analysis
→ abnormal-event investigation
→ data-quality verification
→ grounded AI-assisted explanation
```

The engineering platform must make it possible to answer market questions while preserving:

- source lineage;
- data grain;
- source revisions;
- data-quality context;
- temporal coverage;
- processing history.

---

# 6. Business Questions

The MVP must support the following questions.

## BQ01 — Peak Ontario Demand

> What was Ontario's peak electricity demand for a selected day?

Required domain:

```text
Hourly Demand
```

Expected analytical grain:

```text
market_date + hour
```

---

## BQ02 — Peak Demand Hour

> At what hour did peak demand occur?

Requires trusted hourly Ontario Demand observations.

---

## BQ03 — Zonal Demand

> How did demand differ across Ontario zones?

Required domain:

```text
Hourly Zonal Demand
```

Expected analytical grain:

```text
market_date + hour + zone
```

---

## BQ04 — Generation Mix During Peak Demand

> What was the electricity generation mix during peak demand?

Requires integration between:

```text
Hourly Demand
+
Generation by Fuel
```

while preserving their native analytical grains.

---

## BQ05 — Day-Ahead Price

> What was the Day-Ahead Ontario Zonal Price for each hour?

Required domain:

```text
Day-Ahead Ontario Zonal Price
```

Expected grain:

```text
market_date + hour
```

---

## BQ06 — Day-Ahead vs Real-Time Price

> How did Real-Time Ontario Zonal Price differ from Day-Ahead price?

Requires:

```text
Day-Ahead hourly price
+
Real-Time 5-minute price
```

The Real-Time-to-hourly comparison methodology must be explicitly defined before the KPI is implemented.

Missing observations must not be replaced with zero.

---

## BQ07 — Largest DA-vs-RT Deviations

> During which periods was the DA-vs-RT price deviation largest?

Requires a trusted and documented comparison metric.

The metric must operate only where valid observations exist for both participating sources.

---

## BQ08 — Market Context During Large Price Movements

> What was happening with demand and generation when unusually large price movements occurred?

Requires contextual analysis across:

```text
prices
+
Ontario demand
+
generation mix
```

without creating unintended many-to-many multiplication.

---

## BQ09 — Abnormal Market Behaviour

> Are there periods where demand, generation and price moved abnormally relative to recent behaviour?

This question may initially be supported using descriptive and rule-based analytics.

Advanced anomaly detection or machine-learning models are outside the initial five-day engineering sprint unless required later.

---

## BQ10 — Trustworthiness

> Is the underlying data sufficiently complete and fresh to trust the analysis?

This requires operational evidence rather than a subjective answer.

Potential evidence includes:

```text
source freshness
completeness
grain uniqueness
schema validation
source coverage
DQ results
pipeline execution
source revisions
```

---

# 7. Functional Requirements

## FR-001 — Multi-Source Ingestion

The platform must ingest the five initial IESO sources:

```text
Hourly Demand
Hourly Zonal Demand
Generator Output by Fuel Type Hourly
Day-Ahead Hourly Ontario Zonal Price
Real-Time Ontario Zonal Price
```

---

## FR-002 — Raw Source Preservation

GridPulse must preserve original source payloads in Bronze.

Raw source evidence must remain available for:

- reproducibility;
- reprocessing;
- source comparison;
- debugging.

---

## FR-003 — Source Revision Detection

GridPulse must identify when previously known logical source data changes.

Filename alone must not be used as the only revision identifier.

Payload hashes and source version information must be used where applicable.

---

## FR-004 — Trusted Silver Layer

GridPulse must transform raw source structures into normalized Silver datasets with:

- explicit schema;
- explicit grain;
- normalized column names;
- technical lineage metadata;
- appropriate null handling;
- validation.

---

## FR-005 — Data Quality

The platform must evaluate reusable data-quality rules covering relevant categories such as:

```text
schema
required fields
grain uniqueness
completeness
validity
freshness
cross-source reconciliation
```

DQ results must use:

```text
PASS
WARN
FAIL
```

---

## FR-006 — Invalid Data Handling

Invalid data must not disappear silently.

Where appropriate:

```text
valid
→ trusted Silver
```

```text
structurally invalid
→ quarantine
```

```text
valid but suspicious
→ preserve + DQ observation
```

---

## FR-007 — Analytical Gold Layer

The platform must expose business-oriented analytical datasets without combining incompatible grains into a single mega-table.

Expected facts include:

```text
fact_market_hourly
fact_generation_hourly
fact_zonal_demand_hourly
fact_realtime_price_5min
```

---

## FR-008 — Near-Real-Time Price Ingestion

The platform must support a near-real-time path for Real-Time Ontario Zonal Price observations.

Target conceptual flow:

```text
IESO
→ Python publisher
→ Fabric Eventstream
→ Eventhouse
→ KQL
```

---

## FR-009 — Operational Observability

The platform must record operational metadata for:

- ETL runs;
- source payloads;
- revisions;
- DQ execution.

Conceptual operational datasets:

```text
ops.etl_run
ops.source_file_registry
ops.dq_result
```

---

## FR-010 — AI-Assisted Investigation

The final platform should allow an AI investigation layer to retrieve trusted information through controlled tools.

The language model must not freely invent or independently calculate critical KPIs.

Conceptually:

```text
AI
→ SQL tools
→ KQL tools
→ metadata / DQ tools
```

---

## FR-011 — Insufficient Evidence Handling

When trusted data is not available, the investigation layer must explicitly indicate insufficient evidence.

Expected behaviour:

> I don't have sufficient data to answer this question.

---

# 8. Data Requirements

## DR-001 — Initial Analytical Period

Initial project scope:

```text
2026-01-01
→ latest trusted available observation
```

This is a project-level analytical boundary.

It does not imply identical source coverage.

---

## DR-002 — Source-Specific Coverage

Each source retains its actual trusted historical coverage.

Cross-source analysis uses the intersection of available trusted observations.

GridPulse must not fabricate missing historical observations.

---

## DR-003 — Explicit Grain

Every trusted dataset must expose a documented business grain.

Current expected Silver grains:

```text
Demand
market_date + hour_ending

Zonal Demand
market_date + hour_ending + zone

Generation
market_date + hour_ending + fuel_type

Day-Ahead Price
market_date + hour_ending

Real-Time Price
delivery_date + delivery_hour + interval
```

---

## DR-004 — Lineage

Trusted records must remain traceable to their originating source payload.

Relevant metadata includes:

```text
source
source file
source hash
source version
ingestion timestamp
run ID
```

---

# 9. Non-Functional Requirements

## NFR-001 — Reproducibility

The platform must allow trusted datasets to be rebuilt from preserved source payloads and documented transformations.

---

## NFR-002 — Idempotency

Repeated processing of unchanged source data must not create duplicate trusted observations.

---

## NFR-003 — Maintainability

Reusable ingestion, validation and transformation logic should be preferred over source-specific code duplication where practical.

---

## NFR-004 — Observability

Pipeline success must be distinguishable from data-quality success.

Operational metadata must provide evidence of both.

---

## NFR-005 — Explainability

Important architectural decisions and business transformations must be documented sufficiently to explain:

```text
what was decided
why it was decided
what alternatives existed
what trade-offs were accepted
```

---

## NFR-006 — Security

Current source data is classified as:

```text
PUBLIC
```

Observed PII classification:

```text
NONE
```

Credentials, secrets, tokens, connection strings and API keys must never be committed to the public repository.

---

## NFR-007 — Performance

The MVP prioritizes:

```text
correctness
maintainability
reproducibility
```

before premature optimization.

Techniques such as:

```text
partitioning
OPTIMIZE
compaction
caching
```

must only be introduced when measurable workload behaviour justifies them.

---

## NFR-008 — Version Control

Engineering documentation and relevant implementation artifacts must be version controlled through Git/GitHub.

Microsoft Fabric Git integration will be introduced where supported and appropriate.

---

# 10. MVP Scope

The five-day engineering sprint includes:

```text
architecture
source discovery
Bronze
Silver
Gold
data quality
observability
orchestration
near-real-time ingestion
KQL
grounded AI investigation
testing
production-readiness review
```

---

# 11. Out of Scope for the Five-Day Sprint

The following are intentionally deferred:

- advanced Power BI dashboard;
- complete Direct Lake semantic model;
- executive reporting;
- deep exploratory analysis;
- advanced forecasting;
- weather enrichment;
- sophisticated anomaly detection;
- economic optimization;
- LinkedIn publication;
- demo video;
- final CV bullet.

Deferring these items prevents analytical polish from displacing core platform-engineering work.

---

# 12. Business Acceptance Criteria

The MVP is considered functionally successful when it can demonstrate that:

### AC-001

A source payload can be traced from IESO through Bronze and into a trusted dataset.

### AC-002

The business grain of every trusted fact dataset is explicitly documented and validated.

### AC-003

Reprocessing an unchanged source does not create duplicate trusted data.

### AC-004

A revised source can be detected and preserved.

### AC-005

Data-quality results can explain whether analytical data is trustworthy.

### AC-006

Demand, generation and price can be analyzed together without unintended grain multiplication.

### AC-007

Real-Time Ontario price observations can reach the Real-Time Intelligence path.

### AC-008

KQL can retrieve useful current/recent price information.

### AC-009

The investigation layer can retrieve trusted SQL/KQL/DQ evidence through tools.

### AC-010

Unsupported or insufficiently evidenced questions do not produce fabricated answers.

---

# 13. Success Criterion

A technical reviewer should be able to follow:

```text
business requirement
        ↓
architecture decision
        ↓
source contract
        ↓
implementation
        ↓
data-quality evidence
        ↓
trusted analytical result
```

The project's success is therefore not measured by the number of Microsoft Fabric features used.

It is measured by whether each component solves an explicit engineering or business requirement and whether the resulting data can be trusted.
