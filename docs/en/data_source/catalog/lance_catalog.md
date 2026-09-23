---
sidebar_position: 160
displayed_sidebar: docs
description: Query Lance datasets using a read-only external catalog and catalog-scoped object storage credentials.
---

# Lance catalog

The Lance connector is experimental. It reads existing datasets through the Lance Rust SDK and the Arrow C Data Interface. Deploy the FE planner, BE connector, and native reader library together. Catalog discovery alone does not provide SQL execution.

## Create a catalog

Register each dataset URI and its schema in the catalog properties. Schema declarations must match the dataset. For example:

```sql
CREATE EXTERNAL CATALOG lance_data
PROPERTIES (
    "type" = "lance",
    "database" = "datasets",
    "table.vectors.uri" = "s3://example-bucket/vectors.lance",
    "table.vectors.schema" = "id:int64,embedding:fixed_size_list<float32,128>",
    "aws.s3.access_key" = "<access-key>",
    "aws.s3.secret_key" = "<secret-key>",
    "aws.s3.region" = "us-east-1"
);

SELECT id FROM lance_data.datasets.vectors WHERE id > 10 LIMIT 20;
```

For temporary S3 credentials, also set `aws.s3.session_token`. S3-compatible endpoints use `aws.s3.endpoint`, `aws.s3.enable_path_style_access`, and `aws.s3.enable_ssl`. The FE sends the catalog cloud configuration to the BE, where the reader converts it to Lance storage options.

For the native credential chain on the BE, use `aws.s3.use_aws_sdk_default_behavior = true`. This uses Lance's native object-store credential resolution, rather than the Java AWS SDK. Explicit catalog STS role assumptions, `aws.s3.use_instance_profile`, and `aws.s3.use_web_identity_token_file` are not supported and fail explicitly.

## Azure Data Lake Storage Gen2

Use an account-qualified dataset URI and a SAS token with permission to read and list the dataset files:

```sql
CREATE EXTERNAL CATALOG lance_azure
PROPERTIES (
    "type" = "lance",
    "database" = "datasets",
    "table.events.uri" = "abfss://data@exampleaccount.dfs.core.windows.net/events.lance",
    "table.events.schema" = "id:int64,event_time:timestamp[us]",
    "azure.adls2.storage_account" = "exampleaccount",
    "azure.adls2.sas_token" = "<sas-token>"
);

SELECT id, count(*)
FROM lance_azure.datasets.events
GROUP BY id;
```

Azure shared keys, standard ADLS2 managed identity, and ADLS2 service-principal credentials are also translated from the catalog cloud configuration. Managed identity must be available to the BE host. Service-principal authentication currently requires the public Azure login authority. Workload-identity token files, custom Azure storage endpoints, and account-less `az://` URIs with catalog credentials are not supported. Unsupported or mismatched configurations fail rather than silently discarding the catalog credentials.

SAS tokens and temporary S3 credentials are static catalog configuration; the connector does not obtain or refresh them from a remote credential service. Replace expiring credentials before starting new queries. Never place credentials in SQL predicates or dataset URI query strings.

## Query behavior and limits

- Register multiple datasets with additional `table.<name>.uri` and `table.<name>.schema` properties. They can participate in joins, aggregations, filters, and projections.
- Each table scan uses one range covering every dataset fragment. Fragment-level parallel scans and predicate pushdown into Lance are not implemented. StarRocks evaluates SQL predicates on the decoded rows.
- Column pruning reads the projected columns and columns required by predicates. `COUNT(*)` retains a scalar column when available.
- Arrow large strings and large binary values are supported. Declare them as `large_string` (or `large_utf8`) and `large_binary`. Unsigned integers widen without overflow: `uint8` → `SMALLINT`, `uint16` → `INT`, `uint32` → `BIGINT`, and `uint64` → `DECIMAL(20,0)`.
- Scalar values, dates, timestamps, and lists are supported by the reader. Dates and timestamps retain UTC semantics, including fractional values before the Unix epoch. Unsupported schema conversions fail the query instead of dropping rows.
- The catalog is read-only. Local file URIs must be accessible on the BE selected for the scan.
- The native reader is packaged as `be/lib/libstarrocks_lance.so` on Linux. Lance scans do not require a Java reader or JVM.

To build without Lance, pass `--without-connector-lance` to `build.sh`. This disables the BE connector, skips the Rust build, and excludes its native library from the BE package.

The reader opens a dataset once and keeps that immutable snapshot for all batches of a scan. The FE does not yet pin a common version across separate scan operators, including self-joins.

Native builds use Rust 1.96.1 or newer. If a suitable toolchain is unavailable on Linux x86-64 or AArch64, the build downloads and verifies a pinned official toolchain into the BE build directory. The Rust dependency graph is locked in `Cargo.lock`. Set `LANCE_BUILD_JOBS` to bound Rust compilation parallelism (default: 2). Offline builds require a preinstalled toolchain and a populated Cargo dependency cache.
