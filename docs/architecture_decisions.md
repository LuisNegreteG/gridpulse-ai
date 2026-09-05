# GridPulse AI — Architecture Decision Records

**Project:** GridPulse AI — Ontario Real-Time Energy Intelligence & DataOps Platform  
**Last reviewed:** 2026-09-04

---

## Purpose

This document records architectural decisions that materially affect the design, implementation, maintainability, or operation of GridPulse AI.

An ADR is created only when a decision involves meaningful alternatives or trade-offs.

Implementation details that do not materially affect the architecture should not become ADRs.

Each ADR records:

- context;
- decision;
- alternatives considered;
- consequences;
- current status.

Possible statuses:

```text
PROPOSED
ACCEPTED
SUPERSEDED
DEFERRED
REJECTED
```

---

# ADR-001 — Use 2026 as the Initial Analytical Period

**Status:** ACCEPTED  
**Date:** 2026-08-15

## Context

GridPulse analyzes Ontario electricity-market information from several IESO sources, including:

- Hourly Demand;
- Hourly Zonal Demand;
- Generation by Fuel;
- Day-Ahead Ontario Zonal Price;
- Real-Time Ontario Zonal Price.

The project must compare demand, generation, Day-Ahead prices, and Real-Time prices without silently combining data produced under materially different market structures.

The initial MVP therefore requires a clearly bounded analytical period.

## Decision

GridPulse will use:

```text
2026-01-01
through
latest trusted available observation
```

as the initial analytical period.

Data from earlier market regimes will not be mixed into the MVP without explicit modeling and documentation.

## Alternatives Considered

### Alternative A — Use all historical data available

Rejected for the MVP.

Advantages:

- larger historical dataset;
- more opportunities for long-term trend analysis.

Disadvantages:

- greater risk of combining incompatible market concepts;
- additional normalization requirements;
- increased complexity unrelated to the initial business questions.

### Alternative B — Start at the earliest available Day-Ahead/Real-Time price date

Not selected.

This would maximize price coverage but would create inconsistent starting periods across the other annual datasets.

### Alternative C — Use calendar year 2026

Selected.

It provides a simple and explicit initial market regime boundary while supporting the MVP business questions.

## Consequences

Positive:

- clearer analytical semantics;
- simpler cross-source integration;
- easier explanation of the MVP scope;
- lower risk of silently mixing different market regimes.

Negative:

- reduced historical depth;
- some analyses cannot yet evaluate multi-year behaviour.

## Future Review

Historical periods may be added later as separately modeled market regimes.

Any such extension must explicitly document compatibility and transformation rules.

---

# ADR-002 — Preserve Raw Source Payloads in Bronze

**Status:** ACCEPTED  
**Date:** 2026-08-15

## Context

IESO publishes data in multiple formats:

```text
CSV
XML
```

Source reports may include:

- metadata preambles;
- nullable values;
- revised files;
- source-specific structures;
- unexpected but valid values.

Transforming source data immediately without retaining the original payload would reduce reproducibility and make reconciliation difficult.

## Decision

GridPulse will preserve original source payloads in the Bronze layer.

Bronze is treated as:

```text
raw
immutable
replayable
source evidence
```

Bronze paths use:

```text
Files/bronze/ieso/demand/
Files/bronze/ieso/demand_zonal/
Files/bronze/ieso/generation/
Files/bronze/ieso/price_day_ahead/
Files/bronze/ieso/price_realtime/
```

Business transformations are not applied to Bronze payloads.

## Alternatives Considered

### Alternative A — Parse source files and persist only structured Delta tables

Rejected.

Advantages:

- simpler storage layout;
- less raw-file management.

Disadvantages:

- loss of source evidence;
- more difficult reprocessing;
- difficult forensic investigation;
- reduced ability to reproduce parser behaviour after schema changes.

### Alternative B — Keep source files only temporarily

Rejected.

This would reduce long-term storage but weaken lineage and source-revision analysis.

### Alternative C — Immutable Bronze preservation

Selected.

## Consequences

Positive:

- complete lineage;
- reproducibility;
- reprocessing capability;
- easier debugging;
- stronger auditability;
- source-version comparison.

Negative:

- additional storage;
- requires lifecycle and naming discipline.

## Engineering Rule

Conceptually:

```text
source bytes
→ calculate SHA-256
→ preserve bytes in Bronze
→ transform downstream
```

Source values are never silently corrected in Bronze.

---

# ADR-003 — Detect Source Revisions Using Version Metadata and Payload Hashes

**Status:** ACCEPTED  
**Date:** 2026-08-15

## Context

Source discovery demonstrated that IESO can publish revised files.

Some reports expose explicit versions such as:

```text
_v1
_v2
_vN
```

Other sources use mutable filenames or aliases.

Therefore:

```text
filename already processed
```

does not necessarily mean:

```text
source payload has not changed
```

The Real-Time report provides an especially important example because its current-report alias is intentionally mutable.

## Decision

GridPulse will distinguish:

```text
file identity
```

from:

```text
payload identity
```

using source metadata together with a SHA-256 payload hash.

Where available, lineage will include:

```text
source_name
source_url
source_file
source_version
source_created_at
file_size
source_hash
first_seen_timestamp
last_seen_timestamp
run_id
processing_status
```

## Revision Logic

Conceptually:

```text
same logical source
+ same payload hash
→ unchanged source payload
```

```text
same logical source
+ different payload hash
→ source revision
```

A source revision must be preserved rather than overwriting the previous Bronze payload.

## Alternatives Considered

### Alternative A — Deduplicate only by filename

Rejected.

This fails for mutable aliases and revised files.

### Alternative B — Deduplicate only by explicit `_vN` version

Rejected.

Not every source exposes versioning consistently through the filename.

### Alternative C — Hash-aware source registry

Selected.

## Consequences

Positive:

- robust idempotency;
- source revisions can be audited;
- replayability;
- reduced risk of missing corrected source data.

Negative:

- additional operational metadata;
- hashing adds small processing overhead.

At the current project scale, correctness is more important than this minor overhead.

---

# ADR-004 — Separate Analytical Tables by Native Grain

**Status:** ACCEPTED  
**Date:** 2026-08-15

## Context

GridPulse integrates datasets with different business grains.

Examples:

```text
Demand
date + hour

Day-Ahead price
date + hour

Generation
date + hour + fuel

Zonal demand
date + hour + zone

Real-Time price
date + hour + interval
```

Combining these datasets directly into one large fact table would introduce repeated values and potentially create many-to-many multiplication.

## Decision

GridPulse will preserve distinct analytical fact tables by grain.

Planned Gold datasets include:

```text
gold.fact_market_hourly
```

Grain:

```text
market_date + hour
```

---

```text
gold.fact_generation_hourly
```

Grain:

```text
market_date + hour + fuel_type
```

---

```text
gold.fact_zonal_demand_hourly
```

Grain:

```text
market_date + hour + zone
```

---

```text
gold.fact_realtime_price_5min
```

Grain:

```text
five-minute market interval
```

## Alternatives Considered

### Alternative A — Single denormalized mega-table

Rejected.

Example problem:

```text
10 zones
×
8 generation categories
×
12 RT intervals
```

could multiply one hourly market observation into hundreds of rows.

This would distort metrics such as demand and price unless every downstream query carefully compensated for the duplication.

### Alternative B — Aggregate everything to hourly grain

Rejected as the sole model.

It would discard useful five-minute Real-Time price information.

### Alternative C — Separate facts by native analytical grain

Selected.

## Consequences

Positive:

- metrics retain semantic correctness;
- easier dimensional modeling;
- clearer contracts;
- lower many-to-many risk;
- downstream consumers understand table purpose.

Negative:

- some analytical questions require multiple facts;
- additional SQL/tool logic may be required for cross-domain analysis.

This complexity is preferable to incorrect metrics.

---

# ADR-005 — Use One Schema-Enabled Lakehouse for the MVP

**Status:** ACCEPTED  
**Date:** 2026-08-15

## Context

GridPulse requires logical separation between:

```text
Bronze
Silver
Gold
Operational metadata
```

One possible architecture would use multiple physical Lakehouses:

```text
lh_gridpulse_bronze
lh_gridpulse_silver
lh_gridpulse_gold
lh_gridpulse_ops
```

However, the MVP has moderate volume and is being implemented as one cohesive platform.

Creating multiple physical Lakehouses at this stage would introduce additional management complexity without a demonstrated requirement.

## Decision

GridPulse will initially use one schema-enabled Fabric Lakehouse:

```text
lh_gridpulse
```

Logical separation will be achieved through:

```text
Files/bronze/
```

and Lakehouse schemas:

```text
silver
gold
ops
```

Conceptually:

```text
lh_gridpulse
│
├── Files/
│   └── bronze/
│
├── silver.*
├── gold.*
└── ops.*
```

## Alternatives Considered

### Alternative A — Separate Lakehouse per Medallion layer

Not selected for the MVP.

Potential advantages:

- stronger physical isolation;
- independent ownership/security boundaries;
- independent lifecycle management.

Current disadvantages:

- unnecessary infrastructure complexity;
- additional configuration;
- harder portfolio implementation without a measurable requirement.

### Alternative B — One Lakehouse without schemas

Rejected.

Schemas provide clearer logical organization and naming.

### Alternative C — One schema-enabled Lakehouse

Selected.

## Consequences

Positive:

- simpler development;
- lower operational overhead;
- clear logical organization;
- easier navigation;
- sufficient isolation for the current use case.

Negative:

- less physical isolation;
- a future enterprise implementation may need separate data domains, workspaces, or storage boundaries.

## Future Review

Multiple Lakehouses may become justified if GridPulse later requires:

- independent security boundaries;
- separate teams or ownership;
- independent deployment lifecycles;
- materially different workload characteristics;
- independently governed data products.

Complexity will be introduced when a requirement justifies it.

---

# ADR-006 — Select AI Agent Implementation After Validating Available Fabric Capabilities

**Status:** DEFERRED  
**Date:** 2026-08-15

## Context

GridPulse requires an AI-assisted investigation capability that can access:

```text
Gold / SQL analytics
Real-Time KQL analytics
data-quality metadata
source metadata
```

Two broad implementation approaches are possible:

```text
native Microsoft Fabric AI capability
```

or:

```text
externally implemented tool-calling agent
```

The appropriate solution depends partly on the capabilities available in the active Fabric environment at implementation time.

The project explicitly avoids selecting a technology merely to demonstrate it.

## Decision

No final AI-agent implementation is selected during Day 1.

The decision is intentionally deferred until Day 5.

Before implementation GridPulse will verify current Microsoft documentation and the capabilities available in the active Fabric environment.

The agent architecture must satisfy the same functional requirements regardless of implementation:

```text
AI Agent
│
├── SQL tools
├── KQL tools
└── Metadata / DQ tools
```

Critical KPIs must be calculated by trusted tools rather than freely generated by the language model.

## Required Behaviour

The agent must:

- select appropriate tools;
- provide tool parameters;
- interpret returned evidence;
- produce grounded responses;
- refuse unsupported conclusions.

Expected fallback:

```text
I don't have sufficient data to answer this question.
```

## Alternatives Considered

### Alternative A — Commit immediately to native Fabric AI

Deferred.

Availability and suitability must be verified at implementation time.

### Alternative B — Commit immediately to a custom external agent

Deferred.

A custom implementation may provide greater control but adds code and operational complexity.

### Alternative C — Preserve the tool contract and defer implementation choice

Selected for now.

## Consequences

Positive:

- avoids designing around an unverified platform capability;
- keeps the architecture portable;
- prevents unnecessary technology coupling;
- allows implementation to follow actual requirements.

Negative:

- final AI architecture remains unresolved during the initial engineering phases.

## Resolution Criteria

Before this ADR can become `ACCEPTED`, GridPulse must evaluate:

```text
current Fabric capabilities
active environment availability
SQL tool integration
KQL tool integration
control over tool execution
grounding behaviour
evaluation support
implementation complexity
```

The final choice will update this ADR rather than creating an undocumented implementation decision.

---

# ADR-007 — Treat Historical Coverage as Source-Specific and Use Trusted Coverage Intersections

**Status:** ACCEPTED  
**Date:** 2026-08-25

## Context

Source discovery showed that historical availability differs across IESO reports.

Annual sources such as demand and generation can expose broad year-to-date coverage.

Price sources use different publication and retention behaviours.

In particular, Day-Ahead source discovery demonstrated that older requested 2026 daily files were not necessarily available through the current public-report directory even though newer dates were available.

Real-Time price data also has its own source-specific historical availability.

Therefore, the project-level analytical period:

```text
2026-01-01 → latest
```

does not imply that every individual source contains observations for every point within that period.

## Decision

GridPulse will treat historical coverage as a property of each source.

No dataset will manufacture missing observations to satisfy the global analytical period.

Cross-source analyses operate over the trusted intersection of the participating datasets.

For example:

```text
DA-vs-RT valid analytical period
=
trusted Day-Ahead coverage
∩
trusted Real-Time coverage
```

Similarly:

```text
missing price observation
≠
price = 0
```

## Alternatives Considered

### Alternative A — Force all datasets to the same date range through imputation

Rejected.

This would fabricate market observations.

### Alternative B — Restrict the entire platform to the shortest source history

Rejected.

This would unnecessarily discard valid demand and generation history.

### Alternative C — Preserve source-specific coverage and intersect when required

Selected.

## Consequences

Positive:

- avoids fabricated data;
- transparent analytical coverage;
- each source can retain its maximum useful history;
- DA-vs-RT comparisons remain trustworthy.

Negative:

- business questions involving several sources may have shorter usable periods;
- downstream tools must understand dataset coverage.

## Engineering Implication

Coverage will become observable metadata.

Analytical tools must validate source availability before calculating cross-source KPIs.

Where evidence is insufficient, GridPulse should return an explicit insufficient-data result rather than creating values.


## ADR-008 — Use payload identity for immutable Bronze persistence

**Decision**

Bronze payloads are persisted using SHA-256 content identity rather than filename identity.

Physical paths follow the pattern:

`Files/bronze/ieso/<source>/<sha256>/<original_filename>`

`ops.source_file_registry` maintains logical source identity, payload hash, Bronze path, revision state, and lineage.

**Rationale**

Several IESO sources use mutable annual files or mutable aliases whose filenames remain stable while their contents change.

Using payload identity enables:

- revision detection
- idempotency
- preservation of historical raw evidence
- deterministic lineage
- post-write integrity verification

A filename therefore identifies an upstream source object but does not uniquely identify its payload.


## ADR-009 — Silver represents current trusted state by native business grain

**Decision**

Silver tables maintain one current row per native business key using Delta MERGE.

Incoming matching business keys are updated, new keys are inserted and rows absent from a newer source payload are not automatically deleted.

Historical source revisions remain preserved in Bronze.

**Rationale**

The available source contracts do not establish that absence from a later payload represents an authoritative deletion.

This approach preserves raw history while keeping Silver suitable for current-state analytics and prevents unsupported delete semantics.

## ADR-010 — Preserve source-aligned analytical facts in Gold

**Status:** Accepted

Gold facts preserve the validated analytical grain and independent lineage of each trusted Silver dataset.

Initial Gold facts are:

- `gold.fact_market_demand_hourly`
- `gold.fact_zonal_demand_hourly`
- `gold.fact_generation_hourly`
- `gold.fact_day_ahead_price_hourly`
- `gold.fact_realtime_price_5min`

Cross-source datasets are not materialized unless their grain alignment, business semantics, and trusted coverage intersection are explicitly defined and validated.

This prevents silent grain multiplication, manufactured observations, ambiguous lineage, and coupling between sources with different revision and coverage histories.

Natural business keys are retained. Surrogate keys and dimensions are deferred until a concrete serving requirement justifies them.

## ADR-011 — Use lightweight serving views before derived Gold materialization

**Status:** Accepted

The five source-aligned Gold facts remain the physical analytical serving layer.

Additional cross-source or derived Gold tables are not materialized unless repeated analytical use, performance requirements, or validated business semantics justify physical persistence.

Reusable analytical logic may first be exposed through read-only serving views over Gold facts.

This avoids premature duplication, preserves native grain, and keeps cross-source alignment explicit.

## ADR-012 — Extend ETL run tracking to downstream pipeline executions

**Status:** Accepted

`ops.etl_run` is extended from Bronze ingestion tracking to general GridPulse ETL pipeline execution tracking.

Bronze ingestion runs continue using:

- `pipeline_name = bronze_ingestion`
- one execution per source acquisition attempt

Gold transformations create independent execution IDs using UUID4 and use:

- `pipeline_name = gold_transform`
- one execution per source-aligned Gold dataset transformation

Gold DQ results reference the Gold transformation run ID rather than reusing an upstream Bronze/Silver `_run_id`.

Upstream source lineage remains preserved separately in Gold row-level lineage columns.

This keeps execution lineage distinct from source payload lineage and prevents dataset-level Gold DQ results from being associated with an arbitrary historical ingestion run.

## ADR-013 — Defer the Power BI semantic model until analytical consumption requirements are finalized

**Status:** Accepted

GridPulse does not create a Power BI semantic model during the initial Gold engineering phase.

The current analytical serving layer consists of five source-aligned Gold Delta facts and lightweight SQL serving views.

A semantic model would currently require premature decisions about cross-fact relationships, conformed dimensions, keys, and analytical measures while several cross-source business questions still have incomplete coverage or undefined semantics.

When interactive BI consumption is implemented, the preferred starting architecture is a purpose-built Direct Lake on OneLake semantic model over physical Gold Delta tables.

SQL serving views remain available for SQL consumers and reusable analytical logic but are not required to become semantic-model sources.

Dimensions, additional keys, and relationships will be introduced only when concrete semantic-model requirements justify them.

## ADR-014 — Use Delta Change Data Feed and version watermarks for Gold incremental processing

**Status:** Accepted

Gold incremental processing uses Delta Change Data Feed (CDF) on trusted Silver tables.

Each Gold dataset tracks the last successfully processed Silver Delta version.

A Gold run reads only Silver changes after the stored watermark and up to a fixed ending version captured for that execution.

Changed business keys are resolved back against the current trusted Silver state before Gold MERGE.

The watermark advances only after the Gold transformation and its validation complete successfully.

Because Silver deletion semantics are not defined by ADR-009, unexpected CDF delete events cause the incremental Gold run to fail rather than silently deleting or retaining ambiguous Gold state.

Existing Gold tables provide the validated baseline, so initial watermarks are seeded at the current Silver table versions after CDF is enabled.

## ADR-015 — Use revision-aware Real-Time event identity and history semantics

Status: Accepted

GridPulse publishes SRC-005 Real-Time price events at the native business grain:

`delivery_date + delivery_hour + interval`

Business identity is separated from source revision identity and interval observation identity.

The exact source payload SHA-256 remains the authoritative payload-level lineage identifier. A separate deterministic observation hash identifies whether the semantic state of an individual interval changed. This prevents unchanged intervals from being republished merely because another portion of the mutable XML payload changed.

Published event IDs are deterministic for a given business key and source revision so that publisher or transport retries reproduce the same logical event identity.

Fully populated intervals are eligible for market-price observation events. Fully empty or partially populated intervals are not published as market-price observations. If a previously published eligible interval later becomes ineligible through a legitimate source revision, GridPulse publishes an explicit invalidation event rather than silently retaining stale current state.

Eventhouse retains event history, including legitimate revisions and invalidations. Current eligible Real-Time state is derived through KQL rather than initially maintained as a separate physical table.

The raw source `CreatedAt` value is preserved without timezone inference.

Eventhouse is an analytical Real-Time serving layer and does not replace immutable Bronze source evidence, Silver trusted current state, or source revision lineage.

## ADR-016 — Use a durable outbox and completion checkpoint for Real-Time event publication

Status: Accepted

Date: 2026-09-04

### Context

The SRC-005 Real-Time Ontario Zonal Price report is exposed through a mutable source alias and can change between polling executions.

GridPulse must preserve the following guarantees:

- a successfully acquired source revision must not be lost because downstream transport is unavailable;
- retries must not create a new logical event identity;
- source processing progress must survive notebook or Spark session failures;
- an Eventstream transport failure must not require reacquiring or reparsing the source;
- concurrent or repeated dispatcher executions must not intentionally send the same durable work item simultaneously.

The Lakehouse and Fabric Eventstream cannot participate in one atomic distributed transaction.

Therefore, directly publishing events to Eventstream before recording durable publisher state would create a failure window in which transport and source-processing state could diverge.

### Decision

GridPulse uses a durable Lakehouse outbox and a source-level completion checkpoint for Real-Time publication.

The operational tables are:

- `ops.rt_event_outbox`
- `ops.rt_publisher_checkpoint`

For each source revision, the publisher:

1. acquires the source once and persists exact Bronze evidence;
2. determines revision-aware interval decisions;
3. constructs deterministic Real-Time events;
4. persists publishable events to `ops.rt_event_outbox`;
5. validates that the expected durable events exist;
6. advances the completed source checkpoint;
7. dispatches durable outbox events independently to Fabric Eventstream.

The source checkpoint therefore represents:

> the latest source revision whose publication decisions were durably materialized

It does not mean:

> every derived event has already reached Eventhouse

Transport completion is tracked independently by the outbox.

### Outbox State Model

Durable events use the following transport states:

`PENDING → SENDING → SENT`

`PENDING`

The event is durably stored and eligible for transport.

`SENDING`

A dispatcher has obtained a temporary lease for the event.

The lease records:

- `lease_owner_run_id`
- `lease_expires_at_utc`

An expired `SENDING` lease becomes eligible for recovery by another dispatcher execution.

`SENT`

The transport operation returned successfully and the event records `sent_at_utc`.

A transport exception returns the event to `PENDING`, releases the lease, preserves the deterministic `event_id`, increments attempt metadata, and records `last_error`.

### Delivery Semantics

GridPulse uses at-least-once physical transport semantics.

A failure can occur after Eventstream accepts an event but before the publisher durably records `SENT`.

A later retry may therefore produce more than one physical delivery of the same logical event.

GridPulse does not attempt to solve this with nondeterministic retry IDs.

Instead:

- `event_id` is deterministic;
- Eventhouse retains append-oriented physical history;
- `fn_rt_price_event_dedup()` resolves duplicate physical deliveries by `event_id`;
- downstream current-state functions operate on logical deduplicated events.

Therefore:

`physical deliveries >= logical events`

is allowed, while logical event identity remains stable.

### Checkpoint Semantics

`ops.rt_publisher_checkpoint` tracks source-processing progress separately from transport progress.

Important fields include:

- `last_completed_source_hash`
- `last_completed_bronze_path`
- `last_completed_first_seen_at_utc`
- `last_completed_at_utc`
- `last_successful_poll_at_utc`
- `last_publisher_run_id`

`last_successful_poll_at_utc` may advance even when the source payload is unchanged.

The completed source hash advances only after the revision has been successfully evaluated and all required publishable events have been durably represented in the outbox.

An unchanged source revision does not create duplicate events.

### Alternatives Considered

#### Alternative A — Publish directly to Eventstream and then update the checkpoint

Rejected.

A failure after transport but before checkpoint persistence would cause the source revision to appear unprocessed and could result in uncontrolled replay.

A failure before transport completion could also leave no durable record of work that still needs to be sent.

#### Alternative B — Advance the checkpoint before persisting events

Rejected.

A notebook failure after checkpoint advancement could permanently skip events from a source revision.

#### Alternative C — Treat Eventstream as the durable publisher queue

Rejected.

Eventstream is used as the Real-Time transport and routing layer, not as GridPulse's authoritative publisher control plane.

The Lakehouse outbox preserves retry state, source lineage, event payloads, attempt metadata, and operational auditability.

#### Alternative D — Durable Lakehouse outbox with independent dispatcher

Selected.

This separates source processing from network transport while retaining deterministic recovery behaviour.

### Consequences

Positive:

- source revisions survive downstream transport outages;
- transport retries do not require another source acquisition;
- publisher recovery is deterministic;
- source-processing progress and transport progress remain independently observable;
- expired leases permit recovery from interrupted dispatchers;
- event identity remains stable across retries;
- Eventhouse can safely support at-least-once physical delivery through logical deduplication.

Negative:

- two operational Delta tables must be maintained;
- physical duplicate deliveries remain possible under acknowledged at-least-once semantics;
- dispatcher lease and retry logic add operational complexity;
- unattended dispatch requires the Eventstream credential to be available securely at runtime.

### Deployment Constraint

The current Fabric environment does not expose a supported notebook user-code credential path for the Eventstream/Event Hub connection used by this project.

The notebook therefore expects:

`GRIDPULSE_EVENTSTREAM_CONNECTION`

to be injected securely into the runtime when transport work exists.

The connection string is not stored in source control.

This deployment constraint does not change the durable outbox architecture and may be replaced by a supported managed-identity or secret-injection mechanism in a future deployment environment.


---

# ADR Register


| ADR | Decision | Status |
|---|---|---|
| ADR-001 | Use 2026 as the initial analytical period | ACCEPTED |
| ADR-002 | Preserve raw source payloads in Bronze | ACCEPTED |
| ADR-003 | Detect source revisions using hashes and versions | ACCEPTED |
| ADR-004 | Separate analytical tables by native grain | ACCEPTED |
| ADR-005 | Use one schema-enabled Lakehouse for the MVP | ACCEPTED |
| ADR-006 | Select AI agent implementation after capability validation | DEFERRED |
| ADR-007 | Treat historical coverage as source-specific | ACCEPTED |
| ADR-008 | Use payload identity for immutable Bronze persistence | ACCEPTED |
| ADR-009 | Silver represents current trusted state by native business grain | ACCEPTED |
| ADR-010 | Preserve source-aligned analytical facts in Gold | ACCEPTED |
| ADR-011 | Use lightweight serving views before derived Gold materialization | ACCEPTED |
| ADR-012 | Extend ETL run tracking to downstream pipeline executions | ACCEPTED |
| ADR-013 | Defer the Power BI semantic model until analytical consumption requirements are finalized | ACCEPTED |
| ADR-014 | Use Delta Change Data Feed and version watermarks for Gold incremental processing | ACCEPTED |
| ADR-015 | Use revision-aware Real-Time event identity and history semantics | ACCEPTED |
| ADR-016 | Use a durable outbox and completion checkpoint for Real-Time event publication | ACCEPTED |

---
---

# ADR Governance

New ADRs should be created only for decisions with meaningful architectural consequences.

Good ADR candidates include decisions involving:

```text
storage architecture
data grain
source-of-truth strategy
revision handling
serving architecture
stream processing
security boundaries
deployment architecture
AI architecture
material performance trade-offs
```

Examples of decisions that normally do not require ADRs:

```text
variable names
minor notebook refactoring
temporary debugging techniques
visual formatting
small implementation details
```

An accepted ADR may later change.

When that happens, GridPulse should:

```text
preserve the original ADR
mark it SUPERSEDED
reference the replacement ADR
```

rather than rewriting architectural history.

The ADR log therefore describes not only the final architecture but also the reasoning that produced it.
