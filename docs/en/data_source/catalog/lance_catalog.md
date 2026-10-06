---
sidebar_position: 160
displayed_sidebar: docs
description: Query Lance datasets using a read-only external catalog and catalog-scoped object storage credentials.
---

# Lance catalog

The Lance connector is experimental. The FE uses Lance Java SDK 12.0.0 to discover datasets and read their schemas. The BE reads data through the Lance Rust SDK and the Arrow C Data Interface. Deploy the FE metadata libraries, BE connector, and native reader library together.

## Create a catalog

Configure a directory catalog with the warehouse containing your existing Lance datasets. The connector discovers datasets directly under the warehouse and reads each schema from the Lance format, including column nullability. For example, `vectors.lance` under the following warehouse becomes the table `vectors`:

```sql
CREATE EXTERNAL CATALOG lance_data
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "s3://example-bucket/datasets/",
    "lance.namespace.root_database" = "datasets",
    "aws.s3.access_key" = "<access-key>",
    "aws.s3.secret_key" = "<secret-key>",
    "aws.s3.region" = "us-east-1"
);

SHOW TABLES FROM lance_data.datasets;
SELECT id FROM lance_data.datasets.vectors WHERE id > 10 LIMIT 20;
```

| Property | Description |
| --- | --- |
| `lance.catalog.type` | Catalog implementation. Defaults to `directory`. `rest` is reserved for future support and is currently rejected. |
| `lance.catalog.warehouse` | Required absolute local path or object storage URI containing the Lance datasets. |
| `lance.namespace.root_database` | SQL database name for the warehouse root. Defaults to `default`. This is an alias, not a subdirectory. |

`table.<name>.uri`, `table.<name>.schema`, and the old `database` property are not supported. Nested namespaces are not exposed in this directory implementation. The connector lists existing datasets without creating or updating a namespace manifest.

For temporary S3 credentials, also set `aws.s3.session_token`. S3-compatible endpoints use `aws.s3.endpoint`, `aws.s3.enable_path_style_access`, and `aws.s3.enable_ssl`. The FE sends the catalog cloud configuration to the BE, where the reader converts it to Lance storage options.

For the native credential chain on both FE and BE, use `aws.s3.use_aws_sdk_default_behavior = true`. This uses Lance's native object-store credential resolution, rather than the Java AWS SDK. Explicit catalog STS role assumptions, `aws.s3.use_instance_profile`, and `aws.s3.use_web_identity_token_file` are not supported and fail explicitly.

## Azure Data Lake Storage Gen2

Use an account-qualified dataset URI and a SAS token with permission to read and list the dataset files:

```sql
CREATE EXTERNAL CATALOG lance_azure
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "abfss://data@exampleaccount.dfs.core.windows.net/datasets/",
    "lance.namespace.root_database" = "datasets",
    "azure.adls2.storage_account" = "exampleaccount",
    "azure.adls2.sas_token" = "<sas-token>"
);

SELECT id, count(*)
FROM lance_azure.datasets.events
GROUP BY id;
```

Azure shared keys, standard ADLS2 managed identity, and ADLS2 service-principal credentials are also translated from the catalog cloud configuration. The FE and BE hosts must both have access to storage; managed identity must be available on both hosts. Service-principal authentication currently requires the public Azure login authority. Workload-identity token files, custom Azure storage endpoints, and account-less `az://` URIs with catalog credentials are not supported. Unsupported or mismatched configurations fail rather than silently discarding the catalog credentials.

SAS tokens and temporary S3 credentials are static catalog configuration; the connector does not obtain or refresh them from a remote credential service. Replace expiring credentials before starting new queries. Never place credentials in SQL predicates or dataset URI query strings.

## Query behavior and limits

- Place multiple Lance datasets in the warehouse to query them with joins, aggregations, filters, and projections. Schemas are read when resolving tables, rather than maintained in catalog properties.
- Each table scan uses one range covering every dataset fragment. Fragment-level parallel scans and predicate pushdown into Lance are not implemented. StarRocks evaluates SQL predicates on the decoded rows.
- Column pruning reads the projected columns and columns required by predicates. `COUNT(*)` retains a scalar column when available.
- Arrow large strings and large binary values are supported. Their types are inferred from the dataset schema. Unsigned integers widen without overflow: `uint8` → `SMALLINT`, `uint16` → `INT`, `uint32` → `BIGINT`, and `uint64` → `DECIMAL(20,0)`.
- Scalar values, dates, timestamps, and lists are supported by the reader. Dates and timestamps retain UTC semantics, including fractional values before the Unix epoch. Unsupported schema conversions fail the query instead of dropping rows.
- The catalog is read-only. Local file URIs must be accessible at the same path on the FE and the BE selected for the scan.
- The native reader is packaged as `be/lib/libstarrocks_lance.so` on Linux. BE scans do not require a Java reader or JVM. The FE metadata SDK uses its bundled native library in an isolated class loader.

Lance uses Rust SDK 12.0.0 and is **disabled by default**. Default builds skip Cargo, Rust toolchain downloads, and Lance SDK packaging. Enable it explicitly for the FE metadata libraries and every BE/CN that will execute Lance scans:

```bash
WITH_CONNECTOR_LANCE=ON ./build.sh --fe --be
```

For focused connector tests:

```bash
WITH_CONNECTOR_LANCE=ON ./run-be-ut.sh --build-target connector_lance_test --module connector_lance_test --without-java-ext
```

Direct CMake builds use `-DWITH_CONNECTOR_LANCE=ON`. Set `WITH_CONNECTOR_LANCE=OFF` to explicitly disable it. A default or explicitly disabled build excludes a previously built Lance shared library from the BE package. The enabled FE package includes `fe/lib/lance-metadata-lib`. Default FE packages omit these SDK libraries; directory listing and schema discovery require an enabled FE package. Query execution also requires an enabled BE package.

The reader opens a dataset once and keeps that immutable snapshot for all batches of a scan. The FE does not yet pin a common version across separate scan operators, including self-joins.

Native builds use Rust 1.96.1 or newer. If a suitable toolchain is unavailable on Linux x86-64 or AArch64, the build downloads and verifies a pinned official toolchain into the BE build directory. The Rust dependency graph is locked in `Cargo.lock`. Set `LANCE_BUILD_JOBS` to bound Rust compilation parallelism (default: 2). Offline builds require a preinstalled toolchain and a populated Cargo dependency cache.
