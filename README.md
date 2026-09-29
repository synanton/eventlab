# EventLab

**Deterministic synthetic event and workload generator for large-scale ingestion and search experiments.**

EventLab is a Synanton project for generating large, reproducible streams of synthetic repository events and document metadata.

The primary use case is testing and comparing **ingestion pipelines, search technologies, indexing strategies, and storage backends** on controlled corpora ranging from millions to billions of documents.

## Goals

EventLab is designed to generate:

- `CREATE`, `MERGE`, and `DELETE` events
- deterministic, reproducible document identities and metadata
- configurable metadata distributions
- configurable document/event lifecycles
- temporal event distributions and bursts
- large corpora without requiring the entire dataset in memory
- identical output across repeated and partitioned generation runs

Target corpus sizes:

- **10M documents** — development and integration testing
- **100M documents** — large-scale benchmark
- **1B documents** — scale testing and search experiments

## Reproducibility

Reproducibility is a core requirement.

Given the same:

- generator version
- schema version
- configuration
- seed

EventLab should produce the same event stream **byte-for-byte**.

Generation should also be independent of execution details:

```text
1 worker  ─────────────┐
                       │
8 workers ─────────────┼──► identical event stream
                       │
 partitioned execution ┘
```

This allows benchmark results to be reproduced and different implementations to be compared against exactly the same corpus.

## Event Model

The initial event model is intentionally small:

```text
CREATE
MERGE
DELETE
```

A typical document lifecycle may look like:

```text
CREATE document-123
       │
       ├── MERGE document-123
       │
       ├── MERGE document-123
       │
       └── DELETE document-123
```

Events contain a deterministic document identity, timestamp, operation, metadata, and optional content.

## Metadata

EventLab will support configurable metadata fields such as:

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

Supported field types will include:

- keyword
- text
- integer / numeric
- boolean
- date/time
- multi-valued fields

Metadata distributions should be configurable rather than uniformly random.

For example:

```text
tenant:
  distribution:
    type: categorical
    values:
      tenant-a: 0.70
      tenant-b: 0.20
      tenant-c: 0.10
```

## Event Distributions

EventLab should support configurable operation distributions:

```text
operations:
  create: 0.70
  merge: 0.25
  delete: 0.05
```

More advanced lifecycle rules can define how documents evolve after creation:

```text
lifecycle:
  after_create:
    merge: 0.60
    delete: 0.10
    idle: 0.30
```

## Temporal Workloads

Event generation should support controlled event-rate functions.

Initial candidates include:

- constant rate
- sine-wave rate
- spikes
- periodic bursts
- ramps
- composite functions

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

The purpose is to reproduce different ingestion workload patterns, including steady-state traffic, periodic activity, and sudden bursts.

## Architecture

EventLab is Java-first and should be usable both as a standalone CLI and as a library.

```text
                    EventLab
                       │
             ┌─────────┴─────────┐
             │                   │
          Java API              CLI
             │                   │
             ▼                   ▼
      deterministic          JSONL / ...
        event stream
             │
             ├──────────────► Lucentrix
             │                   │
             │                   ▼
             │               Synanton
             │
             └──────────────► Other tools
```

The core generator should remain independent of Lucentrix.

A dedicated Lucentrix integration can map generated events to the Lucentrix change model.

## Planned Modules

The initial project structure is expected to evolve toward:

```text
eventlab/
├── eventlab-core/
│   ├── event model
│   ├── deterministic generation
│   ├── distributions
│   └── lifecycle
│
├── eventlab-schema/
│   └── workload configuration
│
├── eventlab-cli/
│   └── command-line interface
│
├── eventlab-lucentrix/
│   └── Lucentrix integration
│
├── eventlab-benchmark/
│   └── benchmark scenarios
│
└── docs/
```

## Example Workload

A future workload configuration may look like:

```text
version: 1

seed: 7348291

corpus:
  documents: 100000000

events:
  operations:
    create: 0.70
    merge: 0.25
    delete: 0.05

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

## Output

The initial output format is expected to be JSON Lines:

```json
{"operation":"CREATE","documentId":"doc-000001", "...":"..."}
{"operation":"CREATE","documentId":"doc-000002", "...":"..."}
{"operation":"MERGE","documentId":"doc-000001", "...":"..."}
{"operation":"DELETE","documentId":"doc-000002", "...":"..."}
```

The serialization format must be deterministic when reproducibility is required:

- UTF-8
- stable field ordering
- deterministic number representation
- deterministic timestamp representation
- deterministic escaping

Additional binary formats may be introduced later.

## Benchmark Manifest

Generated datasets should be accompanied by a manifest containing the information necessary to identify the corpus:

```json
{
  "generator_version": "0.1.0",
  "schema_version": 1,
  "seed": 7348291,
  "event_count": 100000000,
  "configuration_sha256": "...",
  "output_format": "jsonl"
}
```

This allows benchmark results to reference a precise, reproducible dataset rather than simply describing it as "100M dummy documents".

## Lucentrix

Lucentrix is the primary integration target.

EventLab should be capable of providing generated events through a Lucentrix source integration:

```text
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

This allows the same deterministic workload to be used for ingestion and search experiments without coupling the generator core to Lucentrix.

## Future: Structured Documents

Structured document generation is intentionally outside the initial scope, but the architecture should support it.

Potential future structures include:

```text
Document
├── metadata
└── structure
    ├── section
    │   ├── paragraph
    │   ├── table
    │   └── list
    └── section
```

This can later support experiments involving:

- hierarchical retrieval
- structured search
- semantic chunking
- table retrieval
- relationships
- GraphRAG

## Initial Roadmap

### Phase 1 — Deterministic Core

-  Java 21 project
-  Event model
-  deterministic PRNG
-  seed-based generation
-  deterministic document IDs
-  `CREATE` / `MERGE` / `DELETE`
-  basic metadata
-  streaming API
-  JSONL output
-  CLI
-  reproducibility tests

### Phase 2 — Distribution Engine

-  categorical distributions
-  weighted distributions
-  uniform distributions
-  configurable operation distribution
-  document lifecycle
-  deterministic timestamps

### Phase 3 — Workload Scheduler

-  constant rate
-  sine function
-  spikes
-  periodic bursts
-  ramps
-  composite workload functions

### Phase 4 — Lucentrix Integration

-  Lucentrix source plugin
-  `ChangePage` integration
-  direct Synanton ingestion
-  ingestion metrics

### Phase 5 — Scale Validation

-  10M benchmark
-  100M benchmark
-  1B scale experiment
-  generation throughput measurements
-  memory measurements
-  CPU measurements
-  reproducibility verification

### Phase 6 — Structured Data

-  hierarchical documents
-  tables
-  lists
-  relationships
-  configurable document structure

## Design Principles

1. **Determinism first** — reproducibility is a primary feature.
2. **Streaming** — generation must not require the entire corpus in memory.
3. **Partition independence** — generation must be safely parallelizable.
4. **Configurable distributions** — synthetic data should model controlled workloads, not uniform randomness.
5. **Java-first** — native integration with Java-based Synanton/Lucentrix tooling.
6. **Core independence** — the generator core should not depend on Lucentrix.
7. **Versioned schemas and algorithms** — old benchmark datasets must remain identifiable and reproducible.
8. **Experiment-oriented** — workloads should be designed for measurable engineering experiments.

## Project Status

**Early design / blank project.**

The initial implementation will focus on the deterministic generation core before adding Lucentrix integration and advanced workload modeling.

## Related Projects

- [Synanton Platform](https://github.com/synanton/platform)
- [Lucentrix](https://github.com/synanton/lucentrix)

------

**EventLab** — reproducible synthetic events for large-scale search and ingestion experiments.
