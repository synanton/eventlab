# EventLab

**Deterministic synthetic event/corpus generator for Synanton/Lucentrix experiments.**

EventLab generates large, reproducible streams of synthetic repository events and document metadata for testing and comparing ingestion pipelines, search technologies, indexing strategies, and storage backends.

The project is designed for experiments with corpora ranging from **10M to 1B documents**.

## Goals

EventLab should generate:

- `CREATE`, `MERGE`, and `DELETE` events
- deterministic document identities and metadata
- configurable metadata distributions
- configurable document lifecycles
- configurable temporal workloads
- event bursts and rate patterns
- large corpora using bounded memory
- reproducible output independent of execution parallelism
- workloads suitable for Lucentrix and other ingestion/search tools

Primary target corpus sizes:

- **10M documents** — development and integration testing
- **100M documents** — large-scale benchmarks
- **1B documents** — scale experiments

## Why EventLab?

Search and ingestion experiments need a controlled corpus.

A benchmark should be able to answer:

> Did the implementation change, or did the test data change?

EventLab makes the generated corpus part of the experiment definition.

A workload is identified by:

```text
generator version
+ schema version
+ PRNG algorithm
+ seed
+ workload configuration
+ canonical serialization format
```
The same experiment can therefore be replayed against different:

- ingestion implementations
- search engines
- indexing strategies
- storage backends
- hardware configurations
- software versions

------

# Reproducibility

Reproducibility is a core architectural invariant.

Given the same generator version, schema, PRNG, configuration, and seed:

```text
EventLab
    │
    ├── run #1 ──► corpus A
    │
    └── run #2 ──► corpus B

SHA256(corpus A) == SHA256(corpus B)
```

The generator must not depend on:

- thread scheduling
- wall-clock time
- `Math.random()`
- `ThreadLocalRandom`
- `UUID.randomUUID()`
- platform-specific serialization
- iteration order of unordered collections

Randomness must be derived from deterministic inputs such as:

```text
seed
+ event sequence
+ document identity
+ field identifier
+ draw identifier
```

This allows generation to be parallelized without changing the generated dataset.

------

# Reproducibility Contract

The project will explicitly test the following properties.

### R1 — Same seed

Same:

```text
generator version
schema version
PRNG
configuration
seed
```

must produce identical output.

### R2 — Restartability

Generation can resume from a known event position:

```text
event 0 ... event 4,999,999
event 5,000,000 ... event 9,999,999
```

without changing the resulting stream.

### R3 — Parallel independence

Changing the number of workers must not change generated events:

```text
1 worker
8 workers
32 workers
```

must produce equivalent output.

### R4 — Partition independence

Where supported, independent partitions must be reproducible:

```text
partition 0
partition 1
partition 2
partition 3
```

must correspond exactly to the equivalent event ranges of a single-stream generation.

### R5 — Stable serialization

Canonical serialization must produce identical bytes for identical events.

### R6 — Versioned algorithms

Changes to generation algorithms, distributions, or serialization must be versioned.

Old benchmark datasets must remain identifiable and reproducible.

### R7 — Explicit benchmark identity

Every generated corpus must be associated with a manifest describing exactly how it was generated.

------

# Architecture

EventLab is **Java-first**.

The core generator must remain independent of Lucentrix so that it can be reused by other Synanton tools.

```text
                         EventLab
                            │
              ┌─────────────┴─────────────┐
              │                           │
         Java Library                    CLI
              │                           │
              ▼                           ▼
      Deterministic Event            JSONL / Binary
          Generator                       │
              │                           │
       ┌──────┴────────┐                  │
       ▼               ▼                  │
   Lucentrix       Other tools ◄──────────┘
       │
       ▼
   Synanton
```

Planned modules:

```text
eventlab/
  ├── eventlab-core/
  ├── eventlab-schema/
  ├── eventlab-cli/
  ├── eventlab-lucentrix/
  ├── eventlab-benchmarks/
  └── docs/
```

------

# Event Model

The initial event model is:

```text
  CREATE
  MERGE
  DELETE
```

A generated event contains:

```text
Event
  ├── sequence
  ├── timestamp
  ├── operation
  ├── documentId
  ├── metadata
  └── payload
```

Example:

```json
{
  "operation": "CREATE",
  "documentId": "doc-000001",
  "timestamp": "2026-01-17T14:32:12.000Z",
  "metadata": {
    "tenant": "tenant-a",
    "document_type": "claim",
    "region": "EU",
    "status": "active"
  },
  "content": "..."
}
```

## Document lifecycle

A document may have a lifecycle such as:

```text
CREATE document-123
       │
       ├── MERGE document-123
       │
       ├── MERGE document-123
       │
       └── DELETE document-123
```

`MERGE` and `DELETE` must refer to deterministic document identities.

The exact mechanism for guaranteeing lifecycle validity at very large scale is an explicit **Phase 0 design problem**.

------

# Phase 0 — Architecture Spike

Before implementing the complete lifecycle engine, EventLab must resolve two architectural questions.

## 1. Lifecycle state at 1B scale

The generator must support:

- logically valid `CREATE` / `MERGE` / `DELETE` sequences
- bounded memory
- deterministic generation
- parallel generation
- partition independence

A naïve implementation such as:

```java
Map<DocumentId, DocumentState>
```

for the complete corpus is not acceptable at 1B-document scale.

The design must determine how lifecycle validity can be generated **by construction**, without requiring global in-memory state.

Candidate approaches include:

- deterministic document-first lifecycle generation;
- event-range generation with a deterministic validity oracle;
- document-range partitioning;
- time-bucket generation;
- other stateless or externally materialized approaches.

No approach is selected yet.

## 2. Canonical serialization

Byte-for-byte reproducibility requires a precisely defined serialization format.

For JSONL this includes:

- UTF-8
- no BOM
- LF line endings
- fixed field ordering
- deterministic escaping
- fixed timestamp representation
- deterministic number representation
- no insignificant whitespace

Floating-point calculations must not introduce platform-dependent output.

Where appropriate, EventLab should prefer:

- integer/fixed-point calculations;
- deterministic integer weights;
- explicitly specified mathematical algorithms;
- `StrictMath` where floating-point calculations are unavoidable.

A compact canonical binary representation may become the authoritative reproducibility format, with JSONL treated as an interchange/debug format.

------

# Metadata Generation

EventLab should support configurable metadata fields.

Initial field types:

- keyword
- text
- integer / numeric
- boolean
- date/time
- multi-valued

Example:

```text
metadata:
  fields:

    tenant:
      type: keyword
      distribution:
        type: categorical
        values:
          tenant-a: 0.70
          tenant-b: 0.20
          tenant-c: 0.10

    priority:
      type: integer
      distribution:
        type: uniform
        min: 1
        max: 10
```

Potential fields include:

```text
  document_id
  tenant
  document_type
  category
  region
  language
  status
  author_id
  department
  priority
  created_at
  modified_at
  tags
```

------

# Distributions

EventLab should generate **controlled randomness**, rather than simply uniform random values.

Initial distributions:

- constant
- sequential
- uniform
- categorical
- weighted

Future distributions may include:

- normal
- exponential
- Poisson
- log-normal
- Zipf
- custom distributions

For categorical distributions, integer/fixed-point weights should be preferred where practical to avoid floating-point reproducibility issues.

Example:

```text
tenant:
  distribution:
    type: categorical
    values:
      tenant-a: 7000
      tenant-b: 2000
      tenant-c: 1000
```

------

# Event Distribution

Operation frequencies should be configurable.

Example:

```text
operations:
  create: 70
  merge: 25
  delete: 5
```

The exact interpretation of these values is part of the workload specification.

For lifecycle-aware workloads, operation selection must also respect the current logical state of the affected document.

------

# Temporal Workloads

EventLab should model not only **what** events are generated, but **when** they occur.

Initial workload functions:

- constant
- sine
- spike
- periodic burst
- ramp
- composite

Example:

```text
events/sec

  │              /\
  │             /  \
  │            /    \
  │      _____/      \_____
  │
  └────────────────────────── time
```

Temporal semantics must be explicitly defined.

For example, a workload may specify either:

```text
fixed event count
+ generated timestamps
```

or:

```text
fixed time interval
+ generated event rate
```

The project must avoid ambiguous definitions of `events/sec`.

------

# Workload Configuration

A workload should be represented as a versioned configuration.

Example:

```text
version: 1

seed: 7348291

corpus:
  documents: 100000000

events:
  operations:
    create: 70
    merge: 25
    delete: 5

  rate:
    type: sine
    base: 10000
    amplitude: 7000
    period: 3600

metadata:
  fields:

    tenant:
      type: keyword
      distribution:
        type: categorical
        values:
          tenant-a: 7000
          tenant-b: 2000
          tenant-c: 1000

    priority:
      type: integer
      distribution:
        type: uniform
        min: 1
        max: 10
```

The workload configuration is part of the benchmark identity.

------

# Output

## JSONL

JSONL is the initial human-readable/interchange format:

```json
{"operation":"CREATE","documentId":"doc-000001","...":"..."}
{"operation":"CREATE","documentId":"doc-000002","...":"..."}
{"operation":"MERGE","documentId":"doc-000001","...":"..."}
{"operation":"DELETE","documentId":"doc-000002","...":"..."}
```

It is appropriate for:

- development
- debugging
- integration tests
- smaller datasets
- interoperability

For 100M–1B events, a compact binary format is expected to become important because JSON serialization, storage, and parsing overhead may dominate the experiment.

## Canonical format

The project will define a canonical representation for reproducibility.

Derived formats such as JSONL must not silently change the canonical event representation.

------

# Benchmark Manifest

Every generated dataset should have a manifest.

Example:

```json
{
  "generator_version": "0.1.0",
  "schema_version": 1,
  "prng_algorithm": "splitmix64",
  "seed": 7348291,
  "event_count": 100000000,
  "configuration_sha256": "...",
  "output_format": "jsonl"
}
```

The manifest allows a benchmark to identify exactly which dataset was used.

Future manifests may also contain:

```
canonical_format_version
partition_count
partition_checksums
generator_commit
schema_checksum
```

------

# Lucentrix Integration

Lucentrix is the primary integration target.

The intended architecture is:

```
EventLab
   │
   ▼
Lucentrix SourcePlugin
   │
   ▼
ChangePage
   │
   ▼
Synanton
```

The initial mapping is:

```text
CREATE → ContentChange(CREATE)
MERGE  → ContentChange(MERGE)
DELETE → ContentChange(DELETE)
```

The integration should support:

- deterministic event offsets
- restart/resume
- configurable page/batch size
- ingestion metrics
- partitioned generation

The EventLab core must not depend on Lucentrix.

------

# CLI

The intended CLI will eventually support commands similar to:

```text
eventlab generate \
  --config workload.yaml \
  --seed 7348291 \
  --events 10000000 \
  --output corpus.jsonl
```

Resume:

```text
eventlab generate \
  --config workload.yaml \
  --seed 7348291 \
  --start-event 5000000 \
  --events 5000000
```

The exact CLI is not yet finalized.

------

# Parallel Generation

EventLab should support deterministic parallel generation.

Conceptually:

```
                 Event stream
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   partition 0   partition 1   partition 2
       │             │             │
       ▼             ▼             ▼
    worker 0      worker 1      worker 2
```

The generated event at position `N` must not depend on which worker generated it.

Therefore random state should not be represented as a shared mutable PRNG:

```
// Avoid
Random random = ...
```

Instead, event/field randomness should be derivable from stable coordinates:

```
random(seed, eventSequence, fieldId, drawId)
```

This allows independent generation and reproducible partitioning.

------

# Testing

Reproducibility will be tested using golden datasets and checksums.

Required tests include:

### Same seed

```
run A == run B
```

### Resume

```
first 5M + next 5M == complete 10M
```

### Parallel generation

```
1 worker == 8 workers
```

### Partitioning

```
partition 0 + partition 1 + ...
==
corresponding canonical event range
```

### Serialization

Identical events must produce identical canonical bytes.

### Lifecycle

Generated `MERGE` and `DELETE` events must satisfy the selected lifecycle model.

------

# Planned Roadmap

## Phase 0 — Design Spike

-  resolve 1B-scale lifecycle/state strategy
-  define canonical serialization
-  select PRNG algorithm
-  define deterministic random-access generation
-  define partition semantics
-  define temporal workload semantics
-  finalize reproducibility contract

## Phase 1 — Deterministic Core

-  Java 21 project
-  event model
-  deterministic PRNG
-  seed-based generation
-  deterministic document IDs
-  initial event model
-  basic metadata
-  streaming API
-  JSONL output
-  CLI
-  golden reproducibility tests

## Phase 2 — Lifecycle and Distributions

-  valid `CREATE` / `MERGE` / `DELETE`
-  categorical distributions
-  weighted distributions
-  uniform distributions
-  document lifecycle
-  deterministic timestamps

## Phase 3 — Temporal Workloads

-  constant rate
-  sine
-  spikes
-  periodic bursts
-  ramps
-  composite workloads

## Phase 4 — Lucentrix Integration

-  Lucentrix SourcePlugin
-  `ChangePage` integration
-  restart/resume
-  ingestion metrics
-  partitioned generation

## Phase 5 — Scale Validation

-  10M benchmark
-  100M benchmark
-  1B scale experiment
-  generation throughput
-  memory consumption
-  CPU consumption
-  output size
-  reproducibility verification

## Phase 6 — Structured Documents

-  hierarchical documents
-  sections
-  tables
-  lists
-  relationships
-  configurable document structure

------

# Structured Documents

Structured documents are intentionally a later phase.

The future model may support:

```
Document
  ├── metadata
  └── structure
      ├── section
      │   ├── paragraph
      │   ├── table
      │   └── list
      └── section
```

This could eventually support experiments involving:

- hierarchical retrieval
- structured search
- semantic chunking
- table retrieval
- relationships
- GraphRAG

------

# Design Principles

1. **Determinism first**
    Reproducibility is a primary feature.
2. **Streaming**
    Generation must not require the complete corpus in memory.
3. **Partition independence**
    Parallel execution must not change generated data.
4. **Controlled distributions**
    Synthetic data should model explicit workloads rather than uniform randomness.
5. **Java-first**
    The primary implementation should integrate naturally with Java-based Synanton/Lucentrix tooling.
6. **Core independence**
    The generator core must not depend on Lucentrix.
7. **Version everything that affects reproducibility**
    Generator algorithms, schemas, PRNGs, and serialization formats must be identifiable.
8. **Experiment-oriented**
    Workloads exist to support measurable engineering experiments.
9. **Bounded memory**
    The architecture must remain viable at 1B-document scale.
10. **Explicit semantics**
     Distribution, lifecycle, timestamp, partition, and serialization semantics must be defined rather than implied.

------

# Non-Goals

EventLab is not intended to become:

- a general-purpose data faker;
- a production event generator;
- a distributed message broker;
- a general benchmark framework;
- a replacement for Lucentrix;
- a realistic NLP text-generation system.

Its purpose is:

> **Generate deterministic, configurable, high-volume synthetic event streams and corpora for Synanton experiments.**

------

# Project Status

**Design spike / blank project**

The first implementation should resolve the Phase 0 architectural questions before committing to the complete lifecycle engine.

------

# Related Projects

- [Synanton Platform](https://github.com/synanton/platform)
- [Lucentrix](https://github.com/synanton/lucentrix)

------

**EventLab** — deterministic synthetic events for large-scale ingestion and search experiments.

```
### One change I would make beyond the review

I would **not call JSONL the canonical format yet**. That's the most important subtlety in the review.

The project has two different requirements:

```text
human/interchange format
        ↓
      JSONL

reproducibility primitive
        ↓
canonical deterministic representation
```

Keeping those concepts separate now prevents us from discovering at the 100M/1B scale that JSON serialization, floating-point behavior, or a library upgrade has accidentally changed the benchmark corpus.
