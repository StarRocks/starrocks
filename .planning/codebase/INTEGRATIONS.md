# External Integrations

## Data Lakehouse Formats

| Format | Integration Type | Location |
|--------|------------------|----------|
| Apache Hive | Native connector | `fe/connector/`, `be/src/connector/` |
| Apache Iceberg | Native connector | `fe/connector/iceberg/`, `be/src/connector/` |
| Apache Hudi | Native + JNI | `fe/connector/hudi/`, `java-extensions/hudi-reader/` |
| Delta Lake | Native connector | `fe/connector/deltalake/` |
| Apache Paimon | JNI connector | `java-extensions/paimon-reader/` |

## Storage Systems

### Object Storage
| Service | Protocol | Implementation |
|---------|----------|----------------|
| AWS S3 | S3 API | `fs/s3/` |
| Google Cloud Storage | GCS API | `fs/gcs/` |
| Azure Blob Storage | Azure SDK | `fs/azure/` |
| MinIO | S3-compatible | Via S3 connector |
| Alibaba OSS | OSS API | `fs/oss/` |

### Distributed Storage
| System | Purpose | Location |
|--------|---------|----------|
| HDFS | Hadoop filesystem | `fs/hdfs/`, Hadoop client |
| Apache HBase | NoSQL storage | `connector/hbase/` |
| Apache Kudu | Columnar storage | `connector/kudu/`, `java-extensions/kudu-reader/` |

## Metastore Integrations

| Metastore | Purpose | Location |
|-----------|---------|----------|
| Hive Metastore | Table metadata | `connector/hive/` |
| AWS Glue | Cloud metastore | `connector/hive/` |
| Alibaba DLF | Cloud metastore | `connector/hive/` (dlf-metastore-client) |

## Data Ingestion

| Source | Method | Location |
|--------|--------|----------|
| Apache Kafka | Stream load | `load/routineload/` |
| Apache Pulsar | Stream load | `load/routineload/` |
| JDBC | Data import | `java-extensions/jdbc-bridge/` |
| Spark | Spark DPP | `plugin/spark-dpp/` |
| Flink | Connector | External (community) |

## Authentication & Security

| System | Protocol | Location |
|--------|----------|----------|
| LDAP | Directory service | `authentication/` |
| Kerberos | Authentication | `authentication/`, `credential/` |
| OAuth/OIDC | Modern auth | `authentication/` |
| TLS/SSL | Encryption | HTTP server configuration |

## Protocol Support

| Protocol | Port (Default) | Purpose |
|----------|----------------|---------|
| MySQL Protocol | 9030 | SQL client compatibility |
| HTTP REST API | 8030 (FE), 8040 (BE) | Admin operations |
| Thrift RPC | Various | Internal communication |
| gRPC | Various | Modern RPC (lake service) |

## External Systems

| System | Integration Type | Notes |
|--------|------------------|-------|
| StarOS | Shared-data storage | `staros.version=4.1-rc3` |
| Prometheus | Metrics export | HTTP endpoints |
| Grafana | Visualization | Prometheus-compatible |

## Cloud Integrations

| Provider | Services |
|----------|----------|
| AWS | S3, Glue, IAM |
| Google Cloud | GCS, BigQuery (via JDBC) |
| Azure | Blob Storage, ADLS |
| Alibaba Cloud | OSS, DLF, MaxCompute (ODPS) |

## Connector Architecture

### FE Connectors (`fe/connector/`)
- Metadata retrieval
- Partition pruning
- Statistics collection
- Plan generation

### BE Connectors (`be/src/connector/`)
- Data reading
- Predicate pushdown
- Column projection
- Vectorized execution

### JNI Connectors (`java-extensions/`)
- Complex format parsing
- Library reuse from Java ecosystem
- Isolated classloading

---
*Mapped: 2026-03-18*
