# GridPulse AI — Source Catalog

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform
**Source owner:** Independent Electricity System Operator (IESO)
**Discovery status:** Day 1 source discovery completed
**Last reviewed:** 2026-08-25
**Data classification:** PUBLIC
**PII:** None observed in the inspected market reports

---

## 1. Purpose

This catalog documents the official IESO data sources used by GridPulse AI.

It records:

* business purpose;
* source format;
* source-level grain;
* publication characteristics;
* historical availability;
* revision behaviour;
* Bronze landing location;
* observed schema characteristics;
* known source quirks;
* expected downstream use.

This document describes source behaviour discovered during profiling. Formal field-level constraints belong in `data_contracts.md`.

GridPulse initially targets the 2026 market period to avoid mixing the current Ontario electricity market structure with the legacy HOEP regime without explicit treatment.

---

## 2. Source Inventory

| Source ID | Source                                      | Format | Source-Level Grain                       | Bronze Location                      | Primary Downstream Target                               |
| --------- | ------------------------------------------- | ------ | ---------------------------------------- | ------------------------------------ | ------------------------------------------------------- |
| SRC-001   | Hourly Demand                               | CSV    | `Date + Hour`                            | `Files/bronze/ieso/demand/`          | `silver.demand_hourly`                                  |
| SRC-002   | Hourly Zonal Demand                         | CSV    | `Date + Hour`                            | `Files/bronze/ieso/demand_zonal/`    | `silver.demand_zonal_hourly`                            |
| SRC-003   | Generator Output by Fuel Type Hourly        | XML    | `Day + Hour + Fuel`                      | `Files/bronze/ieso/generation/`      | `silver.generation_hourly`                              |
| SRC-004   | Day-Ahead Hourly Ontario Zonal Energy Price | XML    | `DeliveryDate + PricingHour`             | `Files/bronze/ieso/price_day_ahead/` | `silver.price_day_ahead_hourly`                         |
| SRC-005   | Real-Time 5-Min Ontario Zonal Energy Price  | XML    | `DeliveryDate + DeliveryHour + Interval` | `Files/bronze/ieso/price_realtime/`  | `silver.price_realtime_5min` and Real-Time Intelligence |

---

# 3. SRC-001 — Hourly Demand

## Business Purpose

Provides hourly Ontario and market demand information used to support:

* daily peak-demand analysis;
* peak-hour identification;
* market-event investigation;
* demand context for price and generation analysis;
* data completeness and freshness assessment.

## Official IESO Report

**Report name:** Hourly Demand Report
**IESO Public Reports path:** `/public/Demand/`

### 2026 source pattern

`PUB_Demand_2026.csv`

Versioned revisions are also published using patterns such as:

`PUB_Demand_2026_vNNN.csv`

## Format

CSV with a report metadata preamble before the tabular header.

Observed physical structure:

```text
\Hourly Demand Report,,,
\Created at ...,,,
\For 2026,,,
Date,Hour,Market Demand,Ontario Demand
...
```

The tabular header must therefore be detected or handled explicitly rather than assuming that the first physical row is the CSV header.

## Observed Source Columns

* `Date`
* `Hour`
* `Market Demand`
* `Ontario Demand`

## Observed Grain

`Date + Hour`

Discovery validation:

* candidate key was unique;
* no duplicate grain keys were observed.

## Discovery Snapshot

Inspected extract:

* date range: `2026-01-01` through `2026-08-15`;
* rows: `5,425`;
* distinct dates: `227`;
* observed hours: `1–24`;
* null values: `0`;
* invalid dates: `0`;
* duplicate `Date + Hour` keys: `0`;
* missing calendar dates: `0`.

The latest source date contained only one published hourly row when the snapshot was retrieved. Historical dates in the inspected extract contained the expected 24 observed hourly records.

Latest-day completeness must therefore be evaluated separately from historical completeness.

## Source Revision Behaviour

The annual filename is mutable and versioned revisions are published independently.

GridPulse must not interpret:

`filename already processed`

as:

`source payload has never changed`.

SHA-256 payload hashes and source-version metadata will be used for revision detection.

## Known Source Quirks

* CSV metadata rows precede the actual table header.
* Annual/current aliases can change as new data or revisions are published.
* Latest business date may be only partially populated.
* `Market Demand` and `Ontario Demand` are preserved as separate source measures.
* Hour/timezone semantics must not be extended beyond documented source semantics without explicit validation.

---

# 4. SRC-002 — Hourly Zonal Demand

## Business Purpose

Provides hourly electricity demand by Ontario electrical zone and supports:

* zonal-demand comparison;
* regional load analysis;
* investigation of unusual market events;
* reconciliation with provincial demand.

## Official IESO Report

**Report name:** Hourly Zonal Demand Report
**IESO Public Reports path:** `/public/DemandZonal/`

### 2026 source pattern

`PUB_DemandZonal_2026.csv`

Versioned revisions use patterns such as:

`PUB_DemandZonal_2026_vNNN.csv`

## Format

CSV with metadata rows preceding the tabular header.

## Observed Source Columns

* `Date`
* `Hour`
* `Ontario Demand`
* `Northwest`
* `Northeast`
* `Ottawa`
* `East`
* `Toronto`
* `Essa`
* `Bruce`
* `Southwest`
* `Niagara`
* `West`
* `Zone Total`
* `Diff`

## Observed Physical Grain

`Date + Hour`

The physical source uses a **wide representation**, with individual zones represented as columns.

## Intended Silver Grain

`market_date + hour + zone`

Silver will therefore perform a controlled wide-to-long transformation.

The Bronze representation remains unchanged.

## Discovery Snapshot

Inspected extract:

* date range: `2026-01-01` through `2026-08-15`;
* rows: `5,448`;
* distinct dates: `227`;
* observed hours: `1–24`;
* null values: `0`;
* duplicate `Date + Hour` keys: `0`;
* missing calendar dates: `0`;
* historical incomplete dates: `0`.

`227 × 24 = 5,448`, consistent with complete hourly coverage for the inspected period.

## Zone Total and Diff Investigation

Observed arithmetic relationships were investigated but are **not currently treated as contractual business rules**.

Across the inspected extract:

* the sum of displayed zone values did not always exactly equal displayed `Zone Total`;
* the arithmetic difference between displayed `Zone Total` and `Ontario Demand` did not always exactly equal displayed `Diff`;
* observed deviations were small.

The pattern may be consistent with independent source rounding, but this explanation has not been confirmed from authoritative IESO documentation.

GridPulse therefore preserves the original values and does not silently recompute them.

## Cross-Source Reconciliation Finding

`Ontario Demand` was compared against SRC-001 using `Date + Hour`.

Results:

* overlapping records: `5,448`;
* exact matches: `5,447`;
* mismatches: `1`;
* maximum absolute difference: `1,270`.

Observed exception:

```text
Date: 2026-03-20
Hour: 1

Hourly Demand Report:
Ontario Demand = 15,232

Hourly Zonal Demand Report:
Ontario Demand = 13,962

Difference = 1,270
```

Root cause: **Not confirmed.**

Engineering treatment:

* preserve both source values;
* preserve source lineage and versions;
* expose the discrepancy through data-quality/reconciliation monitoring;
* do not silently overwrite either source;
* do not classify the record as malformed solely because the sources disagree.

---

# 5. SRC-003 — Generator Output by Fuel Type Hourly

## Business Purpose

Provides hourly IESO-reported generation output grouped by source category and supports:

* generation-mix analysis;
* peak-demand investigation;
* market-event analysis;
* demand/generation/price contextual analysis.

## Official IESO Report

**Report name:** Generator Output by Fuel Type Hourly Report
**IESO Public Reports path:** `/public/GenOutputbyFuelHourly/`

### 2026 source pattern

`PUB_GenOutputbyFuelHourly_2026.xml`

Versioned revisions use patterns such as:

`PUB_GenOutputbyFuelHourly_2026_vNNN.xml`

## Format

XML.

### Declared XML namespace

`http://www.ieso.ca/schema`

### Source XSD

`/docrefs/schema/GenOutputbyFuelHourly_r1.xsd`

## Observed XML Hierarchy

```text
Document
├── DocHeader
└── DocBody
    ├── DeliveryYear
    └── DailyData
        ├── Day
        └── HourlyData
            ├── Hour
            └── FuelTotal
                ├── Fuel
                └── EnergyValue
                    ├── OutputQuality
                    └── Output
```

## Observed Grain After XML Flattening

`Date + Hour + Fuel`

No duplicate candidate grain keys were observed.

## Discovery Snapshot

Inspected extract:

* date range: `2026-01-01` through `2026-08-15`;
* flattened rows: `38,160`;
* `DailyData` elements: `227`;
* `HourlyData` elements: `5,448`;
* duplicate `Date + Hour + Fuel` keys: `0`;
* missing calendar dates: `0`;
* `Output` nulls: `9`;
* `OutputQuality` nulls: `0`.

## Observed Source Categories

Eight distinct values were observed:

* `BIOFUEL`
* `CONTROL ACTIONS`
* `GAS`
* `HYDRO`
* `NUCLEAR`
* `OTHER`
* `SOLAR`
* `WIND`

Most inspected hours contained seven `FuelTotal` elements.

All 24 hours of `2026-07-15` contained eight values because the additional `CONTROL ACTIONS` category was present.

The XSD defines `FuelTotal` with `maxOccurs="unbounded"`. GridPulse therefore does not hard-code an assumption of exactly seven generation categories.

## Output and OutputQuality

The XSD defines:

* `EnergyValue` as optional;
* `Output` as optional;
* `OutputQuality` as an integer;
* `OutputQuality` documentation as a quality flag indicating the number of unavailable data points.

Nine observed records had a missing `Output`.

All nine corresponded to:

```text
Date: 2026-07-15
Fuel: CONTROL ACTIONS
Hours: 1–9
OutputQuality: -1
Output: null
```

From Hour 10 onward, `CONTROL ACTIONS` contained `Output = 0` and `OutputQuality = 0` in the inspected sample.

GridPulse does not infer a complete semantic mapping for negative `OutputQuality` values without authoritative documentation.

Therefore:

* raw quality codes are preserved;
* missing `Output` is not automatically converted to zero;
* structurally valid null outputs are not automatically quarantined.

## Coverage Limitation

This report represents IESO-reported generation according to the scope of the official source report. Downstream analytics and AI responses must not describe it more broadly than the source supports.

---

# 6. SRC-004 — Day-Ahead Hourly Ontario Zonal Energy Price

## Business Purpose

Supports:

* hourly Day-Ahead Ontario price analysis;
* DA-vs-RT price comparison;
* identification of large price deviations;
* market-event investigation.

## Official IESO Report

**Report name:** Day-Ahead Hourly Ontario Zonal Energy Price Report
**IESO Public Reports path:** `/public/DAHourlyOntarioZonalPrice/`

### File pattern

```text
PUB_DAHourlyOntarioZonalPrice_YYYYMMDD.xml
```

Revised files can also exist:

```text
PUB_DAHourlyOntarioZonalPrice_YYYYMMDD_vN.xml
```

Multiple revisions were observed for some dispatch dates.

## Format

XML.

### Declared XML namespace

`http://www.ieso.ca/schema`

### Source XSD

`/docrefs/schema/DAHourlyOntarioZonalPrice_r1.xsd`

## Observed XML Structure

```text
Document
├── DocHeader
└── DocBody
    ├── DeliveryDate
    └── HourlyPriceComponents
        ├── PricingHour
        ├── ZonalPrice
        ├── LossPriceCapped
        ├── CongestionPriceCapped
        └── Flag
```

## Observed Grain

`DeliveryDate + PricingHour`

## Discovery Sample

Sample file:

`PUB_DAHourlyOntarioZonalPrice_20260816.xml`

Observed:

* delivery date: `2026-08-16`;
* records: `24`;
* unique grain keys: `24`;
* duplicate keys: `0`;
* observed PricingHour domain: `1–24`;
* null values: `0`.

## XSD Constraints

`PricingHour` uses `Hour1To24`, explicitly constrained from `1` through `24`.

Price fields use `empty_or_decimal`.

Therefore, the following fields are nullable by source contract even though no nulls were observed in the inspected sample:

* `ZonalPrice`
* `LossPriceCapped`
* `CongestionPriceCapped`

The XSD also constrains monetary values to two decimal places.

## Observed Price Behaviour

The inspected sample contained negative values in loss and congestion components.

Negative values must therefore **not** be rejected by a generic `price >= 0` data-quality rule.

## Flag

Observed value in the inspected sample:

`DSO-RD`

The source XSD states that `Flag` indicates whether the data is administered or produced from DSO-RD.

The observed value is not treated as the complete contractual domain.

## Historical Availability

Current public-report availability was observed to be limited/rolling rather than equivalent to the annual YTD files available for Demand and Generation.

Direct probes of older 2026 files returned HTTP `404`, while newer dispatch dates were available.

Version probes confirmed that source revisions exist.

Exact contractual historical retention for this report has **not been confirmed**.

GridPulse therefore:

* records source-specific availability;
* does not fabricate missing historical coverage;
* compares DA and RT only across periods available in both trusted datasets.

---

# 7. SRC-005 — Real-Time 5-Min Ontario Zonal Energy Price

## Business Purpose

Primary near-real-time source for:

* five-minute Ontario price monitoring;
* DA-vs-RT comparison;
* unusual price-movement detection;
* real-time market investigation;
* Eventstream/Eventhouse ingestion.

## Official IESO Report

**Report name:** Real-Time 5-Min Ontario Zonal Energy Price Report
**IESO Public Reports path:** `/public/RealtimeOntarioZonalPrice/`

### Mutable current-report alias

`PUB_RealtimeOntarioZonalPrice.xml`

IESO documents a report range of 90 days for this source.

## Format

XML.

### Declared XML namespace

`http://www.ieso.ca/schema`

### Source XSD

`/docrefs/schema/RealtimeOntarioZonalPrice_r2.xsd`

## Publication Characteristics

IESO publishes this source every five minutes for the current dispatch hour.

The report exposes twelve five-minute interval slots.

## Observed XML Structure

```text
Document
├── DocHeader
│   └── CreatedAt
└── DocBody
    ├── DeliveryDate
    ├── DeliveryHour
    ├── ZonalPrice × up to 12
    │   ├── Interval
    │   ├── Flag
    │   ├── LmpCap
    │   ├── LossPriceCap
    │   └── CongPriceCap
    └── AveragePrice
        ├── LmpCap
        ├── LossPriceCap
        └── CongPriceCap
```

## Observed Candidate Grain

`DeliveryDate + DeliveryHour + Interval`

## Discovery Snapshot

Snapshot retrieval:

`2026-08-25T22:43:50Z`

Source metadata:

```text
DeliveryDate: 2026-08-25
DeliveryHour: 18
Source CreatedAt: 2026-08-25T17:37:57
DocRevision: 1
```

Observed:

* interval slots: `12`;
* unique grain keys: `12`;
* duplicate keys: `0`;
* interval domain: `1–12`;
* fully populated slots: `9`;
* fully empty slots: `3`;
* partially populated slots: `0`.

## Nullable Price Components

The XSD defines the following as `empty_or_decimal`:

* `LmpCap`
* `LossPriceCap`
* `CongPriceCap`

Therefore, empty prices are structurally valid.

A slot existing in the XML does **not** necessarily mean that a market price result has already been published for that interval.

## Semantic Mapping

GridPulse maps source fields conceptually as follows:

```text
LmpCap        -> capped Ontario Zonal Price
LossPriceCap  -> capped loss component
CongPriceCap  -> capped congestion component
```

Source field names remain available through lineage.

## AveragePrice

`AveragePrice` is a separate source-level aggregate and is not modeled as a thirteenth five-minute interval.

In the inspected snapshot, reported averages matched the arithmetic mean of the nine populated intervals rounded to two decimals.

This is recorded as **observed behaviour**, not as a confirmed contractual calculation rule.

## Mutable Alias and Revision Handling

`PUB_RealtimeOntarioZonalPrice.xml` is a mutable latest-state artifact.

Bronze snapshots therefore use:

```text
retrieval timestamp
+
payload SHA-256
```

rather than the source filename alone.

Example pattern:

```text
PUB_RealtimeOntarioZonalPrice
__retrieved_YYYYMMDDTHHMMSSZ
__sha256_<hash>.xml
```

## Eventstream Implication

GridPulse will distinguish:

**Business key**

```text
delivery_date
+ delivery_hour
+ interval
```

from:

**Source/event revision**

```text
business key
+ source_created_at
+ source_hash
```

A five-minute interval becomes eligible for Eventstream publication only when the required price components are populated.

Empty future/unavailable slots remain preserved in the source snapshot but are not emitted as market events.

This is a GridPulse engineering rule, not an IESO source rule.

---

# 8. Common Source-Handling Requirements

All five sources follow these GridPulse ingestion principles.

## Raw Preservation

Bronze preserves source payloads without business transformation.

For raw files:

```text
source bytes
→ SHA-256
→ immutable Bronze payload
```

## Revision Detection

Source identity must not rely exclusively on filename.

GridPulse will consider:

* source name;
* source URL;
* filename;
* explicit source version where available;
* payload SHA-256;
* source creation timestamp where available;
* ingestion timestamp.

## Idempotency

Conceptual processing rule:

```text
same source identity + same hash
→ do not process again

same source filename + different hash
→ source revision detected
→ preserve and process new version
```

## Invalid Data

Invalid records must not disappear silently.

Possible treatments include:

```text
valid data
→ Silver

structurally invalid data
→ quarantine

valid but semantically suspicious data
→ preserve + DQ finding
```

Cross-source disagreement is not automatically equivalent to malformed data.

## Freshness

Freshness is source-specific.

It may require a combination of:

* source `CreatedAt`;
* maximum business date;
* maximum business hour/interval;
* latest-date record count;
* ingestion timestamp.

A maximum date alone is insufficient to determine whether a source is complete and current.

---

# 9. Source Discovery Status

| Source                        | Raw Integrity | Schema/Structure | Candidate Grain | Null Profile | Revision Behaviour | Status |
| ----------------------------- | ------------- | ---------------- | --------------- | ------------ | ------------------ | ------ |
| Hourly Demand                 | PASS          | PASS             | PASS            | PROFILED     | OBSERVED           | READY  |
| Hourly Zonal Demand           | PASS          | PASS             | PASS            | PROFILED     | OBSERVED           | READY  |
| Generation by Fuel Hourly     | PASS          | PASS             | PASS            | PROFILED     | OBSERVED           | READY  |
| Day-Ahead Ontario Zonal Price | PASS          | PASS             | PASS            | PROFILED     | OBSERVED           | READY  |
| Real-Time Ontario Zonal Price | PASS          | PASS             | PASS            | PROFILED     | OBSERVED           | READY  |

All five MVP sources are sufficiently understood to proceed to draft Data Contracts.

---

# 10. Open Items

The following items remain deliberately unresolved until additional authoritative evidence or implementation work is available:

1. Exact timezone/timestamp derivation rules for converting source date/hour/interval fields into canonical market timestamps.
2. Contractual interpretation of all negative `OutputQuality` values in the Generation report.
3. Formal semantics behind `Zone Total` and `Diff` arithmetic discrepancies in Hourly Zonal Demand.
4. Root cause of the `2026-03-20 Hour 1` cross-source Ontario Demand discrepancy.
5. Exact contractual historical retention of the Day-Ahead Ontario Zonal Price public-report files.
6. Whether additional source revisions require business-specific precedence rules beyond latest trusted source version/hash.
7. Final Eventstream publication and revision-processing behaviour, to be implemented and tested during Real-Time Intelligence development.

These items must not be resolved through assumptions.
