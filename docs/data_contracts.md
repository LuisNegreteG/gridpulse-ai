# GridPulse AI — Data Contracts

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform  
**Contract version:** 0.1  
**Status:** Draft — validated through source discovery  
**Last reviewed:** 2026-08-25  

---

## 1. Purpose

This document defines the initial data contracts between:

1. IESO public source reports;
2. GridPulse Bronze ingestion;
3. GridPulse Silver datasets;
4. downstream analytical and real-time consumers.

The contracts deliberately separate three concepts:

```text
SOURCE CONTRACT
What the external source publishes

        ↓

SILVER CONTRACT
What GridPulse guarantees after normalization

        ↓

DATA QUALITY RULES
How violations and suspicious conditions are classified
```

A behaviour observed during source discovery is not automatically treated as a contractual business rule.

Rules are promoted into the contract only when they are supported by one or more of the following:

- official source documentation;
- source XSD/schema definitions;
- stable structural characteristics confirmed through profiling;
- explicit GridPulse engineering decisions.

The purpose of these contracts is to prevent downstream consumers from depending on undocumented assumptions.

---

## 2. Contract Scope

The current contract covers the five MVP sources used by GridPulse AI:

| Source ID | GridPulse Source Name | Source |
|---|---|---|
| SRC-001 | `ieso_hourly_demand` | Hourly Demand |
| SRC-002 | `ieso_hourly_zonal_demand` | Hourly Zonal Demand |
| SRC-003 | `ieso_generation_by_fuel_hourly` | Generator Output by Fuel Type Hourly |
| SRC-004 | `ieso_day_ahead_ontario_zonal_price` | Day-Ahead Hourly Ontario Zonal Energy Price |
| SRC-005 | `ieso_realtime_ontario_zonal_price` | Real-Time 5-Min Ontario Zonal Energy Price |

The contracts remain at version `0.1` until they are validated against the first complete Bronze-to-Silver implementation.

---

## 3. Contract Principles

### 3.1 Explicit Grain

Every Silver table must define a clear business grain.

Records from incompatible grains must not be combined without an explicit aggregation or modeling decision.

For example:

```text
Hourly Demand
date + hour

Generation
date + hour + fuel

Zonal Demand
date + hour + zone

Real-Time Price
date + hour + interval
```

Joining these datasets must not create unintended row multiplication.

---

### 3.2 Bronze Is Immutable Source Evidence

Bronze preserves the original source payload.

GridPulse does not silently:

- correct source values;
- replace null values;
- recalculate source fields;
- remove suspicious records;
- overwrite revised source payloads.

Bronze exists to support:

- reproducibility;
- reprocessing;
- auditability;
- reconciliation;
- forensic investigation.

---

### 3.3 Nullable Does Not Mean Invalid

If the source schema explicitly permits a value to be empty, a null value is not automatically considered malformed.

Structural validity and analytical completeness are treated separately.

For example:

```text
valid XML + nullable Output
≠
invalid record
```

---

### 3.4 Missing Does Not Mean Zero

GridPulse must never infer:

```text
missing value
→ 0
```

unless an authoritative business rule explicitly supports that transformation.

This applies particularly to:

- generation output;
- Day-Ahead prices;
- Real-Time prices;
- missing historical source coverage.

---

### 3.5 Negative Prices Are Valid Values

Electricity-market price components may be negative.

GridPulse must not apply a generic rule such as:

```text
price >= 0
```

to Day-Ahead or Real-Time price data.

---

### 3.6 Cross-Source Disagreement Is Not Automatic Corruption

Two structurally valid IESO reports may disagree.

GridPulse preserves both values and surfaces the discrepancy through reconciliation controls.

A mismatch does not justify silently selecting one source as correct.

---

### 3.7 Revision Awareness

A source filename does not necessarily identify a unique payload.

Conceptually:

```text
same filename + same hash
→ same payload

same filename + different hash
→ revised payload
```

Source revision information and SHA-256 hashes therefore form part of GridPulse lineage.

---

### 3.8 No Unsupported Business Rules

GridPulse does not assume that:

```text
all days always contain 24 published rows
fuel categories always equal seven
all price values must exist
all source aggregates must reconcile exactly
the latest date is necessarily complete
```

unless authoritative source evidence supports the rule.

---

## 4. Common Technical Metadata Contract

Silver datasets include common lineage metadata where applicable.

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `_source_name` | string | No | Logical GridPulse source identifier |
| `_source_file` | string | No | Source filename or immutable Bronze snapshot name |
| `_source_url` | string | Yes | Original source URL |
| `_source_hash` | string | No | SHA-256 hash of the raw source payload |
| `_source_version` | string | Yes | Explicit source revision/version when available |
| `_source_created_at` | timestamp | Yes | Timestamp provided by the source report |
| `_ingestion_timestamp` | timestamp | No | UTC timestamp when GridPulse ingested the payload |
| `_run_id` | string | No | GridPulse processing-run identifier |

Additional source-specific metadata may be retained when required.

The `_source_hash` always represents the raw source payload and not a transformed record.

---

## 5. Data Quality Severity Model

GridPulse uses three primary DQ outcomes.

### PASS

The applicable contract requirement is satisfied.

A valid but unusual business value can still pass structural validation.

---

### WARN

The data can be safely preserved and processed, but a condition deserves investigation.

Examples include:

- cross-source disagreement;
- latest-period incompleteness;
- unexpected but structurally valid source category;
- unusual source volume;
- source revision;
- source-permitted null values that reduce analytical completeness.

WARN records are not automatically quarantined.

---

### FAIL

A contractual requirement necessary for safe processing has been violated.

Examples include:

- missing required business key;
- unparseable required date;
- malformed required numeric value;
- invalid structural schema;
- duplicate business key within the same authoritative source payload where uniqueness is required.

FAIL conditions must not silently disappear.

Depending on the rule, GridPulse will either:

```text
quarantine affected records
```

or:

```text
stop processing the affected dataset
```

The decision must be recorded in operational metadata.

---

# 6. SRC-001 — Hourly Demand Contract

## 6.1 Source Identification

**Source ID:** `SRC-001`  
**GridPulse source name:** `ieso_hourly_demand`  
**Source format:** CSV  

Observed source pattern:

```text
PUB_Demand_YYYY.csv
```

Versioned revisions may also exist.

---

## 6.2 Source Contract

### Physical Source Columns

```text
Date
Hour
Market Demand
Ontario Demand
```

### Physical Source Grain

```text
Date + Hour
```

### Source Characteristics

- report metadata precedes the actual CSV header;
- the table header must therefore be detected or explicitly located;
- annual/current source files evolve as data is published;
- revised versions may be published;
- the latest business date may be incomplete at retrieval time.

### Discovery Evidence

The inspected 2026 extract contained:

```text
Rows                     : 5,425
Dates                    : 227
Date range               : 2026-01-01 → 2026-08-15
Duplicate Date+Hour keys : 0
Null values              : 0
Invalid dates            : 0
```

Historical dates contained complete observed hourly coverage.

The latest date contained only one row at the time of retrieval.

---

## 6.3 Silver Contract

### Target

```text
silver.demand_hourly
```

### Silver Grain

```text
market_date + hour_ending
```

### Silver Fields

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `market_date` | date | No | Source `Date` |
| `hour_ending` | integer | No | Source `Hour` |
| `market_demand_mw` | numeric | No | Source `Market Demand` |
| `ontario_demand_mw` | numeric | No | Source `Ontario Demand` |
| common metadata | — | — | GridPulse lineage fields |

### Mapping

```text
Date
→ market_date

Hour
→ hour_ending

Market Demand
→ market_demand_mw

Ontario Demand
→ ontario_demand_mw
```

No business-value imputation is performed.

Timezone semantics will not be added until explicitly validated.

---

## 6.4 Data Quality Rules

### DEMAND-001 — Required Business Key

Required:

```text
market_date IS NOT NULL
hour_ending IS NOT NULL
```

Severity: `FAIL`

---

### DEMAND-002 — Grain Uniqueness

Within one authoritative source payload/version:

```text
market_date + hour_ending
```

must be unique.

Severity: `FAIL`

---

### DEMAND-003 — Hour Domain

Observed and expected source domain:

```text
1–24
```

Values outside the accepted source domain cannot safely satisfy the Silver contract.

Severity: `FAIL`

---

### DEMAND-004 — Required Demand Measures

The inspected source contained no null demand values.

An unparseable or missing demand measure is therefore treated as a Silver contract failure.

Severity: `FAIL`

The original Bronze payload remains preserved.

---

### DEMAND-005 — Historical Daily Completeness

Unexpected incompleteness in a completed historical business date must be surfaced.

Severity: `WARN`

It is not automatically treated as malformed source data because calendar/time semantics must be respected.

---

### DEMAND-006 — Latest-Day Completeness

The most recent business date may still be accumulating observations.

Latest-day incompleteness:

Severity: `WARN`

not automatic `FAIL`.

---

### DEMAND-007 — Source Revision

A previously processed filename with a different payload hash indicates a revised source.

Severity: `WARN`

Engineering action:

```text
preserve revision
process revision
record lineage
```

---

# 7. SRC-002 — Hourly Zonal Demand Contract

## 7.1 Source Identification

**Source ID:** `SRC-002`  
**GridPulse source name:** `ieso_hourly_zonal_demand`  
**Source format:** CSV  

Observed source pattern:

```text
PUB_DemandZonal_YYYY.csv
```

---

## 7.2 Source Contract

### Physical Source Grain

```text
Date + Hour
```

### Physical Representation

The report uses a wide representation.

Observed fields:

```text
Date
Hour
Ontario Demand
Northwest
Northeast
Ottawa
East
Toronto
Essa
Bruce
Southwest
Niagara
West
Zone Total
Diff
```

### Discovery Evidence

The inspected extract contained:

```text
Rows                     : 5,448
Dates                    : 227
Date range               : 2026-01-01 → 2026-08-15
Duplicate Date+Hour keys : 0
Historical incomplete    : 0
Null values              : 0
```

Observed:

```text
227 × 24 = 5,448
```

---

## 7.3 Silver Contract

### Target

```text
silver.demand_zonal_hourly
```

### Silver Grain

```text
market_date + hour_ending + zone
```

### Silver Fields

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `market_date` | date | No | Source date |
| `hour_ending` | integer | No | Source hour |
| `zone` | string | No | Ontario electrical zone |
| `demand_mw` | numeric | No | Demand reported for the zone |
| common metadata | — | — | GridPulse lineage fields |

### Wide-to-Long Transformation

The following source columns are unpivoted:

```text
Northwest
Northeast
Ottawa
East
Toronto
Essa
Bruce
Southwest
Niagara
West
```

into:

```text
zone
demand_mw
```

---

## 7.4 Source Aggregates

The following source fields do not have zone-level grain:

```text
Ontario Demand
Zone Total
Diff
```

They must therefore not be duplicated into every zone-level Silver record.

They remain available through:

- Bronze;
- staging/reconciliation logic;
- DQ processing.

---

## 7.5 Data Quality Rules

### ZONAL-001 — Required Silver Grain

Required:

```text
market_date
hour_ending
zone
```

Severity: `FAIL`

---

### ZONAL-002 — Silver Grain Uniqueness

Within one authoritative source payload:

```text
market_date + hour_ending + zone
```

must be unique.

Severity: `FAIL`

---

### ZONAL-003 — Hour Domain

Accepted hour domain:

```text
1–24
```

Severity: `FAIL` for values outside the accepted domain.

---

### ZONAL-004 — Zone Demand Parsing

Zone demand must be parseable as a numeric value.

Malformed or absent required zone demand:

Severity: `FAIL`

---

### ZONAL-005 — Zone Schema Evolution

GridPulse does not assume that the initially observed set of zone columns can never change.

A previously unseen zone or changed source structure must be detected.

Severity:

```text
WARN
```

if it can be safely interpreted.

Severity:

```text
FAIL
```

if the transformation can no longer determine the source structure safely.

---

### ZONAL-006 — Zone Total Reconciliation

The sum of displayed zone values may be compared with source `Zone Total`.

Discovery found that they do not always reconcile exactly.

Therefore exact equality is not currently a hard source contract.

Mismatch:

Severity: `WARN`

No source value is overwritten.

---

### ZONAL-007 — Diff Reconciliation

The relationship between:

```text
Zone Total
Ontario Demand
Diff
```

is monitored.

Discovery showed small arithmetic discrepancies in some records.

Because authoritative calculation semantics have not been confirmed, exact equality is not a hard contract.

Severity: `WARN`

---

### ZONAL-008 — Cross-Source Ontario Demand Reconciliation

SRC-002 `Ontario Demand` is compared against SRC-001 `Ontario Demand` using:

```text
market_date + hour_ending
```

Discovery result:

```text
Overlapping records : 5,448
Exact matches       : 5,447
Mismatches          : 1
```

Observed exception:

```text
2026-03-20
Hour 1

SRC-001 Ontario Demand = 15,232
SRC-002 Ontario Demand = 13,962
Difference             = 1,270
```

Root cause: not confirmed.

Mismatch severity: `WARN`

Both source observations are preserved.

---

# 8. SRC-003 — Generator Output by Fuel Type Hourly Contract

## 8.1 Source Identification

**Source ID:** `SRC-003`  
**GridPulse source name:** `ieso_generation_by_fuel_hourly`  
**Source format:** XML  

Observed source pattern:

```text
PUB_GenOutputbyFuelHourly_YYYY.xml
```

Declared source schema:

```text
GenOutputbyFuelHourly_r1.xsd
```

---

## 8.2 Source Contract

### Relevant XML Hierarchy

```text
Document
└── DocBody
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

### Observed Flattened Grain

```text
Date + Hour + Fuel
```

### Discovery Evidence

Observed:

```text
Flattened rows     : 38,160
DailyData elements : 227
HourlyData         : 5,448
Duplicate keys     : 0
Output nulls       : 9
```

Observed source categories:

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

The XSD defines `FuelTotal` with unbounded cardinality.

GridPulse therefore must not hard-code exactly seven or eight categories.

---

## 8.3 Silver Contract

### Target

```text
silver.generation_hourly
```

### Silver Grain

```text
market_date + hour_ending + fuel_type
```

### Silver Fields

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `market_date` | date | No | Market date |
| `hour_ending` | integer | No | Source hour |
| `fuel_type` | string | No | IESO-reported generation category |
| `output_mwh` | numeric | Yes | Reported output |
| `output_quality_code` | integer | No | Raw IESO quality indicator |
| common metadata | — | — | GridPulse lineage fields |

### Semantic Constraint

`fuel_type` represents an **IESO-reported generation category**.

GridPulse does not assume that every value represents a conventional physical fuel.

This is important because the source contains:

```text
CONTROL ACTIONS
```

---

## 8.4 Output Nullability

The XSD allows `Output` to be absent.

Therefore:

```text
output_mwh IS NULL
```

is structurally valid.

Discovery identified nine such records.

All nine occurred for:

```text
Date: 2026-07-15
Fuel: CONTROL ACTIONS
Hours: 1–9
OutputQuality: -1
```

These records must not be converted to zero.

---

## 8.5 OutputQuality

The XSD describes `OutputQuality` as a quality flag indicating the number of unavailable data points.

Observed codes included:

```text
0
-1
-2
-3
-4
-5
-6
```

Exact semantic labels for individual negative values have not been confirmed.

GridPulse therefore preserves the raw code without inventing interpretations.

---

## 8.6 Data Quality Rules

### GEN-001 — Required Grain

Required:

```text
market_date
hour_ending
fuel_type
```

Severity: `FAIL`

---

### GEN-002 — Grain Uniqueness

Within one authoritative source payload:

```text
market_date + hour_ending + fuel_type
```

must be unique.

Severity: `FAIL`

---

### GEN-003 — Output Nullability

Null `output_mwh` is permitted by the source contract.

Severity:

```text
PASS structurally
WARN analytically when completeness is affected
```

---

### GEN-004 — No Null-to-Zero Transformation

Forbidden implicit transformation:

```text
NULL Output
→ 0
```

A measured zero and an unavailable value are distinct states.

---

### GEN-005 — OutputQuality Preservation

`output_quality_code` must retain the source value exactly.

Unknown codes must not be converted into invented business labels.

Previously unseen structurally valid quality code:

Severity: `WARN`

---

### GEN-006 — Dynamic Fuel Categories

Previously unseen valid `Fuel` value:

Severity: `WARN`

The record should be retained unless another rule makes processing unsafe.

---

### GEN-007 — Output Parsing

A populated `Output` value that cannot be parsed numerically:

Severity: `FAIL`

---

# 9. SRC-004 — Day-Ahead Ontario Zonal Price Contract

## 9.1 Source Identification

**Source ID:** `SRC-004`  
**GridPulse source name:** `ieso_day_ahead_ontario_zonal_price`  
**Source format:** XML  

Observed source pattern:

```text
PUB_DAHourlyOntarioZonalPrice_YYYYMMDD.xml
```

Revisions may use:

```text
PUB_DAHourlyOntarioZonalPrice_YYYYMMDD_vN.xml
```

Declared source schema:

```text
DAHourlyOntarioZonalPrice_r1.xsd
```

---

## 9.2 Source Contract

### Relevant XML Structure

```text
Document
└── DocBody
    ├── DeliveryDate
    └── HourlyPriceComponents
        ├── PricingHour
        ├── ZonalPrice
        ├── LossPriceCapped
        ├── CongestionPriceCapped
        └── Flag
```

### Source Grain

```text
DeliveryDate + PricingHour
```

### XSD Constraints

`PricingHour` uses a type explicitly restricted to:

```text
1–24
```

The monetary fields use:

```text
empty_or_decimal
```

and are therefore nullable by source contract.

The schema defines monetary values with two decimal places.

---

## 9.3 Discovery Evidence

Inspected file:

```text
PUB_DAHourlyOntarioZonalPrice_20260816.xml
```

Observed:

```text
Delivery date   : 2026-08-16
Rows            : 24
Unique keys     : 24
Duplicates      : 0
PricingHour     : 1–24
Null values     : 0
```

Observed ranges:

```text
ZonalPrice
32.10 → 98.14

LossPriceCapped
-0.58 → 0.39

CongestionPriceCapped
-3.72 → 0.25
```

Observed flag:

```text
DSO-RD
```

Negative price components are therefore valid source observations.

---

## 9.4 Silver Contract

### Target

```text
silver.price_day_ahead_hourly
```

### Silver Grain

```text
market_date + hour_ending
```

### Silver Fields

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `market_date` | date | No | Source DeliveryDate |
| `hour_ending` | integer | No | Source PricingHour |
| `zonal_price_cad_per_mwh` | decimal | Yes | Ontario Zonal Price |
| `loss_price_capped_cad_per_mwh` | decimal | Yes | Capped loss component |
| `congestion_price_capped_cad_per_mwh` | decimal | Yes | Capped congestion component |
| `source_flag` | string | Yes | Source administration/origin flag |
| common metadata | — | — | GridPulse lineage fields |

---

## 9.5 Data Quality Rules

### DA-001 — Required Grain

Required:

```text
market_date
hour_ending
```

Severity: `FAIL`

---

### DA-002 — Grain Uniqueness

Within the selected authoritative source revision:

```text
market_date + hour_ending
```

must be unique.

Severity: `FAIL`

---

### DA-003 — Pricing Hour Domain

XSD domain:

```text
1–24
```

Values outside the domain:

Severity: `FAIL`

---

### DA-004 — Nullable Price Fields

The following may be empty according to the source schema:

```text
ZonalPrice
LossPriceCapped
CongestionPriceCapped
```

Null alone is therefore not a structural failure.

Analytical incompleteness may still generate:

Severity: `WARN`

---

### DA-005 — Negative Price Values

Negative prices or price components are not automatically invalid.

No generic positive-only constraint is allowed.

---

### DA-006 — Flag Domain

The XSD defines `Flag` as a string and describes it as indicating whether the value is administered or from DSO-RD.

The discovery sample contained only:

```text
DSO-RD
```

This observed value does not define the complete allowed domain.

Previously unseen valid string:

Severity: `WARN`

---

### DA-007 — Historical Availability

Missing historical source files must not become fabricated zero-price observations.

Missing source coverage is tracked as a source-availability condition.

---

### DA-008 — Source Revision

A later version of the same delivery-date report is a legitimate source revision.

The new payload must be:

```text
preserved
hashed
registered
processed
```

rather than discarded as a duplicate solely because the business date already exists.

---

# 10. SRC-005 — Real-Time Ontario Zonal Price Contract

## 10.1 Source Identification

**Source ID:** `SRC-005`  
**GridPulse source name:** `ieso_realtime_ontario_zonal_price`  
**Source format:** XML  

Mutable current-report alias:

```text
PUB_RealtimeOntarioZonalPrice.xml
```

Declared source schema:

```text
RealtimeOntarioZonalPrice_r2.xsd
```

---

## 10.2 Source Contract

### Relevant XML Structure

```text
Document
├── DocHeader
│   ├── DocRevision
│   └── CreatedAt
│
└── DocBody
    ├── DeliveryDate
    ├── DeliveryHour
    │
    ├── ZonalPrice
    │   ├── Interval
    │   ├── Flag
    │   ├── LmpCap
    │   ├── LossPriceCap
    │   └── CongPriceCap
    │
    └── AveragePrice
        ├── LmpCap
        ├── LossPriceCap
        └── CongPriceCap
```

### Candidate Business Grain

```text
DeliveryDate
+ DeliveryHour
+ Interval
```

### Source Cardinality

The XSD allows:

```text
ZonalPrice maxOccurs="12"
```

The source documentation defines the records as five-minute intervals.

---

## 10.3 XSD Nullability

The following fields use:

```text
empty_or_decimal
```

and therefore permit empty values:

```text
LmpCap
LossPriceCap
CongPriceCap
```

`Flag` uses `xs:string`.

Empty source values must be preserved rather than automatically transformed into zero.

---

## 10.4 Discovery Evidence

Inspected snapshot:

```text
Retrieval UTC : 2026-08-25T22:43:50Z
DeliveryDate  : 2026-08-25
DeliveryHour  : 18
CreatedAt     : 2026-08-25T17:37:57
DocRevision   : 1
```

Observed:

```text
Interval slots       : 12
Unique keys          : 12
Duplicate keys       : 0
Observed intervals   : 1–12
Fully populated      : 9
Fully empty          : 3
Partially populated  : 0
```

The XML contained all twelve interval elements even though the final three slots had no populated price values.

Therefore:

```text
XML slot exists
≠
price observation is currently available
```

---

## 10.5 Source Field Mapping

GridPulse maps the raw source tags conceptually as follows:

```text
LmpCap
→ capped Ontario Zonal Price

LossPriceCap
→ capped loss component

CongPriceCap
→ capped congestion component
```

The raw source tag names remain available through lineage and Bronze preservation.

---

## 10.6 AveragePrice

`AveragePrice` is a separate report-level/hour-level aggregate.

It must not be interpreted as:

```text
Interval = 13
```

In the inspected snapshot:

```text
Reported Zonal average       : 63.27
Calculated populated average : 63.267777...

Reported Loss average        : -0.49
Calculated populated average : -0.487777...

Reported Congestion average        : -0.57
Calculated populated average       : -0.568888...
```

The observed values match arithmetic averages rounded to two decimals.

This remains an **observed behaviour** and not a hard contractual formula until authoritative source documentation confirms it.

---

## 10.7 Silver Contract

### Target

```text
silver.price_realtime_5min
```

### Silver Grain

```text
delivery_date
+ delivery_hour
+ interval
```

### Silver Fields

| Column | Logical Type | Nullable | Description |
|---|---|---:|---|
| `delivery_date` | date | No | Source DeliveryDate |
| `delivery_hour` | integer | No | Source DeliveryHour |
| `interval` | integer | No | Five-minute interval |
| `zonal_price_capped_cad_per_mwh` | decimal | Yes | Source LmpCap |
| `loss_price_capped_cad_per_mwh` | decimal | Yes | Source LossPriceCap |
| `congestion_price_capped_cad_per_mwh` | decimal | Yes | Source CongPriceCap |
| `source_flag` | string | Yes | Source Flag |
| `_source_created_at` | timestamp | Yes | Source report CreatedAt |
| `_source_doc_revision` | string/integer | Yes | Source DocRevision |
| common metadata | — | — | Remaining GridPulse lineage fields |

---

## 10.8 Data Quality Rules

### RT-001 — Required Business Key

Required:

```text
delivery_date
delivery_hour
interval
```

Severity: `FAIL`

---

### RT-002 — Business-Key Uniqueness Per Payload

Within one authoritative source payload:

```text
delivery_date + delivery_hour + interval
```

must be unique.

Severity: `FAIL`

The same business interval appearing in a later legitimate revision is not considered an invalid duplicate.

---

### RT-003 — Delivery Hour Domain

Accepted documented domain:

```text
1–24
```

Unexpected value:

Severity: `FAIL`

---

### RT-004 — Interval Domain

Accepted interval domain:

```text
1–12
```

Unexpected value:

Severity: `FAIL`

---

### RT-005 — Nullable Price Fields

The source XSD explicitly permits empty:

```text
LmpCap
LossPriceCap
CongPriceCap
```

Null values are therefore structurally valid.

---

### RT-006 — Fully Empty Future/Unavailable Slot

If all three price components are null:

```text
zonal_price_capped_cad_per_mwh IS NULL
AND
loss_price_capped_cad_per_mwh IS NULL
AND
congestion_price_capped_cad_per_mwh IS NULL
```

the interval remains structurally valid.

Silver treatment:

```text
preserve
```

Structural severity:

```text
PASS
```

Analytical completeness may be tracked separately.

---

### RT-007 — Partially Populated Price Slot

If some but not all price components are populated:

Severity: `WARN`

until additional authoritative evidence justifies stricter handling.

The original source values remain preserved.

---

### RT-008 — Real-Time Event Publication Eligibility

GridPulse operational rule:

```text
zonal_price_capped_cad_per_mwh IS NOT NULL
AND
loss_price_capped_cad_per_mwh IS NOT NULL
AND
congestion_price_capped_cad_per_mwh IS NOT NULL
```

then:

```text
interval is eligible for Eventstream publication
```

A fully empty slot:

```text
preserve in source snapshot
do not emit as a market-price event yet
```

This is a GridPulse engineering rule, not an IESO source rule.

---

### RT-009 — Revision-Aware Event Identity

Business identity:

```text
delivery_date
+ delivery_hour
+ interval
```

Source/event revision identity additionally includes lineage such as:

```text
source_created_at
source_hash
```

Conceptually:

```text
same business key
+ same source state
→ duplicate event candidate

same business key
+ changed trusted source state
→ legitimate revision
```

A legitimate revised observation must not be silently discarded.

---

### RT-010 — AveragePrice Treatment

`AveragePrice` is not transformed into a normal five-minute Silver interval.

It remains separate source-level metadata or may later support reconciliation/monitoring.

No hard arithmetic DQ formula will be imposed until source semantics are confirmed.

---

### RT-011 — Mutable Alias Snapshot Identity

Because:

```text
PUB_RealtimeOntarioZonalPrice.xml
```

is mutable, Bronze identity cannot depend on filename alone.

GridPulse Bronze snapshot identity includes:

```text
retrieval timestamp
+
payload SHA-256
```

Example:

```text
PUB_RealtimeOntarioZonalPrice
__retrieved_YYYYMMDDTHHMMSSZ
__sha256_<hash>.xml
```

---

# 11. Cross-Source Contracts

## 11.1 Ontario Demand Reconciliation

GridPulse compares:

```text
SRC-001 Ontario Demand
vs
SRC-002 Ontario Demand
```

using:

```text
market_date + hour_ending
```

Exact mismatch:

Severity: `WARN`

Both source observations remain available.

The reconciliation result is recorded rather than silently correcting either source.

---

## 11.2 Day-Ahead vs Real-Time Price Alignment

DA and RT prices may only be compared when trusted observations exist in both datasets.

Forbidden transformation:

```text
missing DA or RT price
→ 0
```

The hourly comparison logic must explicitly define how five-minute Real-Time observations are aggregated.

That aggregation rule will be finalized before Gold implementation.

---

## 11.3 Native Grain Preservation

Native analytical grains remain distinct:

```text
Hourly Demand
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

Gold models must prevent accidental many-to-many multiplication.

---

## 11.4 Historical Coverage Intersection

Source coverage is not assumed to be identical.

Analyses requiring multiple sources operate on the trusted intersection of available coverage unless explicitly stated otherwise.

For example:

```text
DA-vs-RT analysis
=
periods where trusted DA observations exist
∩
periods where trusted RT observations exist
```

Missing source coverage must never be fabricated.

---

# 12. Quarantine Contract

Quarantine is reserved for records that cannot safely satisfy the Silver structural contract.

Potential quarantine conditions include:

```text
unparseable required business key
malformed required numeric field
invalid structural schema
duplicate required key within same source payload
unrecoverable parser failure
```

Quarantine is not the default treatment for every unusual observation.

Examples that normally remain outside quarantine:

```text
negative electricity price
cross-source disagreement
unexpected valid fuel category
latest-day incompleteness
source-permitted null value
new source revision
fully empty future RT interval
```

Every quarantined record must retain enough lineage to identify:

```text
source_name
source_file
source_hash
run_id
DQ rule
failure reason
original payload or record reference
```

Quarantined data must never disappear silently.

---

# 13. Freshness Contract

Freshness is source-specific.

GridPulse must not evaluate freshness using only:

```text
MAX(date)
```

A source may expose the latest business date while still being incomplete.

Freshness evaluation may incorporate:

```text
source CreatedAt
maximum business date
maximum business hour
maximum interval
latest-date record count
expected source cadence
ingestion timestamp
```

The exact implementation will be defined per source during the DQ framework phase.

---

# 14. Source Revision Contract

All ingestion logic must distinguish:

```text
file identity
```

from:

```text
payload identity
```

GridPulse records, where available:

```text
source name
source URL
source filename
explicit source version
payload SHA-256
source CreatedAt
first seen timestamp
last seen timestamp
processing run
processing status
```

Conceptual rules:

```text
same logical source
+ same hash
→ no new payload processing required
```

```text
same logical source
+ different hash
→ revision detected
→ preserve and process
```

No source revision is overwritten in Bronze.

---

# 15. Idempotency Contract

Repeated execution against an unchanged source must not produce duplicate trusted records.

Conceptually:

```text
same payload
+ same transformation contract
→ same logical result
```

Pipeline retries must be safe.

Silver implementation will use appropriate:

- source registry checks;
- business keys;
- payload hashes;
- revision identity;
- Delta MERGE or equivalent idempotent patterns.

Exact implementation will be defined during Bronze/Silver engineering.

---

# 16. Contract Evolution

These contracts are versioned.

A contract change may be required when:

- source schema changes;
- source XSD changes;
- a required field changes;
- a new source column appears;
- grain changes;
- nullability expectations change;
- authoritative business semantics become available;
- downstream compatibility changes;
- GridPulse introduces a new guarantee.

A newly discovered source behaviour must not silently alter the contract.

The expected process is:

```text
detect change
→ preserve raw source
→ assess compatibility
→ update contract
→ update tests
→ implement transformation change
→ document decision
```

Backward-compatible additive changes are preferred when practical.

---

# 17. Relationship to Data Quality Framework

The Data Contract defines:

```text
what should be true
```

The DQ framework implements:

```text
how GridPulse measures whether it is true
```

The operational DQ layer will eventually record results in:

```text
ops.dq_result
```

Expected information includes:

```text
run_id
source_name
dataset_name
rule_id
rule_category
severity
status
records_checked
records_failed
observed_value
expected_value
execution_timestamp
details
```

The final schema will be established during implementation.

---

# 18. Relationship to Run Control

Contract validation must be associated with an ETL run.

Operational execution will use a run-control structure such as:

```text
ops.etl_run
```

Expected information includes:

```text
run_id
pipeline_name
source_name
start_timestamp
end_timestamp
status
records_read
records_written
records_rejected
error_message
```

This makes it possible to distinguish:

```text
pipeline execution succeeded
```

from:

```text
data satisfied quality expectations
```

These are separate operational concepts.

---

# 19. Relationship to Source Registry

Source revisions and payload identity will be tracked through:

```text
ops.source_file_registry
```

Expected information includes:

```text
source_name
source_url
source_file
source_version
file_size
source_hash
source_created_at
first_seen_timestamp
last_seen_timestamp
processing_status
run_id
```

This registry is central to:

- idempotency;
- revision detection;
- lineage;
- reproducibility;
- troubleshooting.

---

# 20. Contract Status by Source

| Contract | Discovery | Grain | Nullability | Silver Mapping | Initial DQ Rules | Status |
|---|---|---|---|---|---|---|
| Hourly Demand | Complete | Defined | Profiled | Defined | Defined | READY FOR IMPLEMENTATION |
| Hourly Zonal Demand | Complete | Defined | Profiled | Defined | Defined | READY FOR IMPLEMENTATION |
| Generation by Fuel | Complete | Defined | XSD + Profiled | Defined | Defined | READY FOR IMPLEMENTATION |
| Day-Ahead Price | Complete | Defined | XSD + Profiled | Defined | Defined | READY FOR IMPLEMENTATION |
| Real-Time Price | Complete | Defined | XSD + Profiled | Defined | Defined | READY FOR IMPLEMENTATION |

---

# 21. Open Contract Questions

The following items remain deliberately unresolved.

## OPEN-001 — Canonical Market Timestamp

GridPulse has validated source-level date/hour/interval fields but has not yet finalized the canonical timestamp transformation.

Open considerations include:

- source timezone;
- hour-ending interpretation;
- five-minute interval boundary semantics;
- daylight-saving-time behaviour.

Until resolved, source market keys are preserved directly.

---

## OPEN-002 — Generation OutputQuality Semantics

The source XSD provides a general description of `OutputQuality`, but GridPulse has not confirmed authoritative semantic labels for every observed negative code.

Raw codes remain preserved.

---

## OPEN-003 — Zonal Demand Aggregate Arithmetic

Exact business semantics behind differences involving:

```text
Zone Total
Ontario Demand
Diff
```

have not been authoritatively confirmed.

The values remain source observations rather than hard arithmetic contracts.

---

## OPEN-004 — Cross-Source Demand Exception

The root cause of the observed discrepancy for:

```text
2026-03-20
Hour 1
```

between the two demand sources remains unconfirmed.

The discrepancy is retained as a reconciliation observation.

---

## OPEN-005 — Day-Ahead Historical Retention

Exact contractual retention for publicly available Day-Ahead files has not been confirmed.

GridPulse records actual source availability instead of assuming complete annual coverage.

---

## OPEN-006 — Real-Time AveragePrice Formula

The inspected Real-Time snapshot showed that `AveragePrice` matched the arithmetic mean of currently populated intervals rounded to two decimals.

This remains observed behaviour rather than an authoritative contractual formula.

---

## OPEN-007 — Final DA-vs-RT Hourly Comparison Rule

Before Gold implementation, GridPulse must explicitly define how five-minute Real-Time observations are aggregated for comparison with hourly Day-Ahead prices.

The rule must be documented before it is used for business KPIs.

---

# 22. Contract Promotion Criteria

Version `0.1` may be promoted after:

- Bronze ingestion is implemented;
- all five Silver transformations are implemented;
- required grain constraints are tested;
- null handling is tested;
- revision handling is tested;
- quarantine behaviour is tested;
- DQ rules are executed end-to-end;
- source lineage is validated;
- contract tests pass.

Until then:

```text
Contract version: 0.1
Status: Draft / Ready for implementation
```

No production-level guarantee is claimed prematurely.

---

# 23. Final Engineering Position

GridPulse treats a data contract as more than a schema.

A trusted dataset requires agreement on:

```text
source identity
+
grain
+
schema
+
nullability
+
semantic meaning
+
revision behaviour
+
quality expectations
+
lineage
+
failure handling
```

The goal is not to force real-world source data into artificial assumptions.

The goal is to provide downstream consumers with explicit, observable, testable guarantees while preserving the original source evidence whenever those guarantee cannot be satisfied.


## Phase 2 Implementation Note

The five planned Silver datasets are now implemented as Delta tables with their previously defined native grains.

Technical lineage implemented across Silver includes:

- `_source_name`
- `_source_file`
- `_source_url`
- `_source_hash`
- `_source_version`
- `_source_created_at`
- `_ingestion_timestamp`
- `_run_id`

Bronze payload identity is based on the SHA-256 hash of the exact raw payload bytes.

Silver uses idempotent Delta MERGE operations by dataset business key. Source rows absent from a subsequent revision are not automatically deleted.

Data quality outcomes are persisted in `ops.dq_result`.

The known SRC-001 / SRC-002 Ontario Demand discrepancy remains preserved as a WARN observation:

- Date: 2026-03-20
- Hour Ending: 1
- SRC-001: 15,232 MW
- SRC-002: 13,962 MW
- Difference: 1,270 MW

Root cause remains unconfirmed

## Gold Serving Contracts

Gold represents the current analytical serving state derived from trusted Silver data.

### gold.fact_market_demand_hourly
Grain: `market_date + hour_ending`

Source:
`silver.demand_hourly`

### gold.fact_zonal_demand_hourly
Grain: `market_date + hour_ending + zone`

Source:
`silver.demand_zonal_hourly`

### gold.fact_generation_hourly
Grain: `market_date + hour_ending + fuel_type`

Source:
`silver.generation_hourly`

`output_mwh` remains nullable when permitted by the upstream source contract.

### gold.fact_day_ahead_price_hourly
Grain: `market_date + hour_ending`

Source:
`silver.price_day_ahead_hourly`

Price fields remain nullable and may contain valid negative values.

### gold.fact_realtime_price_5min
Grain:
`delivery_date + delivery_hour + interval`

Source:
`silver.price_realtime_5min`

Five-minute intervals are not implicitly aggregated to hourly grain.

### Common Gold behavior

- Natural business keys are preserved.
- Gold uses idempotent MERGE semantics.
- Matching keys are updated when trusted Silver state changes.
- New keys are inserted.
- Rows absent from newer Silver state are not automatically deleted.
- Upstream Silver/Bronze lineage is preserved.
- Cross-source datasets use trusted coverage intersections.