# Architecture

## System Overview

StarRocks uses a **decoupled Frontend-Backend (FE-BE)** architecture with support for both **shared-nothing** and **shared-data** deployment modes.

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (FE)                          │
│  Java │ SQL Parsing │ Query Planning │ Metadata Management  │
└────────────────────────────┬────────────────────────────────┘
                             │ Thrift RPC
┌────────────────────────────▼────────────────────────────────┐
│                      Backend (BE)                           │
│  C++ │ Query Execution │ Storage Engine │ Data Processing   │
└─────────────────────────────────────────────────────────────┘
```

## Frontend (FE) Architecture

### Responsibilities
- SQL parsing and validation
- Query planning and optimization (CBO)
- Metadata management
- Cluster coordination
- Access control

### Key Components

| Component | Package | Purpose |
|-----------|---------|---------|
| SQL Parser | `fe/fe-parser/` | ANTLR-based SQL parsing |
| Analyzer | `sql/analyzer/` | Semantic analysis |
| Optimizer | `sql/optimizer/` | Cost-based optimization |
| Planner | `sql/plan/` | Execution plan generation |
| Metadata | `server/`, `meta/` | Table, partition, statistic metadata |
| Catalog | `catalog/` | External data source management |
| Authentication | `authentication/` | User and privilege management |

### Query Flow (FE)
1. **Parse**: SQL → AST
2. **Analyze**: Resolve names, validate semantics
3. **Rewrite**: Transform queries (MV rewrite, etc.)
4. **Optimize**: Cost-based plan selection
5. **Fragment**: Distribute plan to BE nodes

### High Availability
- **Leader/Follower**: FE nodes elect a leader via BDB-JE
- **Journal**: Metadata changes logged to `fe/meta/bdb/`
- **Checkpoint**: Periodic snapshots for fast recovery

## Backend (BE) Architecture

### Responsibilities
- Query execution (vectorized)
- Data storage and management
- Data ingestion
- Compaction and maintenance

### Key Components

| Component | Path | Purpose |
|-----------|------|---------|
| Execution Engine | `src/exec/` | Query operators (scan, join, agg) |
| Expression Eval | `src/exprs/` | Scalar expression evaluation |
| Storage Engine | `src/storage/` | Tablet management, MVCC |
| Columnar Store | `src/column/` | Vectorized data structures |
| Connectors | `src/connector/` | External data reading |
| Runtime | `src/runtime/` | Memory, buffer, thread management |

### Storage Architecture

#### Shared-Nothing Mode (Traditional)
- Data stored locally on BE nodes
- Replication for fault tolerance
- Tablet (partition of table) distributed across nodes

#### Shared-Data Mode (v3.0+)
- Data stored in object storage (S3, etc.)
- BE nodes cache hot data
- Better elasticity, lower cost

### Query Execution
- **Vectorized**: Column-by-column processing
- **MPP**: Parallel execution across nodes
- **Pipeline**: Operator scheduling for efficiency
- **Short-circuit**: Single-table point queries optimized

## Data Flow

### Query Execution Flow
```
Client SQL
    ↓
FE: Parse → Analyze → Optimize → Plan
    ↓
BE: Scan → Filter → Join → Aggregate → Return
    ↓
FE: Merge results → Return to client
```

### Data Ingestion Flow
```
Data Source (Kafka/Stream Load/Broker Load)
    ↓
FE: Coordination, transaction management
    ↓
BE: Write to delta, build indexes
    ↓
Background: Compaction, MV refresh
```

## Materialized Views

| Feature | Implementation |
|---------|----------------|
| Sync MV | Immediate update on base table change |
| Async MV | Scheduled refresh, query rewrite |
| Query Rewrite | Automatic substitution in optimizer |

## Key Design Patterns

### FE Patterns
- **Visitor Pattern**: AST traversal (`SqlVisitor`)
- **Strategy Pattern**: Optimizer rules (`Rule`)
- **Factory Pattern**: Connector creation
- **Observer Pattern**: Metadata change notifications

### BE Patterns
- **Vectorized Execution**: Column-oriented processing
- **Operator Model**: Streaming data flow
- **MVCC**: Multi-version concurrency for tablets
- **RAFT**: Distributed consensus for tablet metadata

## Performance Optimizations

| Optimization | Location | Description |
|--------------|----------|-------------|
| SIMD | BE | AVX2/AVX512 vectorized operations |
| Predicate Pushdown | FE+BE | Filter early at storage layer |
| Partition Pruning | FE | Skip irrelevant partitions |
| Cost-Based Optimization | FE | Choose optimal join order |
| Runtime Filter | BE | Dynamic bloom filter for joins |
| Pipeline Execution | BE | Reduce context switches |
| Data Cache | BE | Hot data in memory |
| Query Cache | FE | Cache query results |

---
*Mapped: 2026-03-18*
