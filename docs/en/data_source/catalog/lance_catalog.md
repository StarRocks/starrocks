---
sidebar_position: 160
displayed_sidebar: docs
description: Query Lance datasets using a read-only external catalog and catalog-scoped object storage credentials.
---

# Lance catalog

The Lance connector is experimental. It reads existing datasets through the Lance JNI reader. Deploy the FE planner, BE connector, and reader JARs together. Catalog discovery alone does not provide SQL execution.

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

## Discover tables with a Lance Namespace REST catalog

Use REST mode to discover namespaces and tables from a service implementing the
[Lance Namespace REST specification](https://lance.org/format/namespace/). No
`table.<name>.uri` or `table.<name>.schema` properties are needed in this mode.

```sql
CREATE EXTERNAL CATALOG lance_rest
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "rest",
    "lance.catalog.uri" = "https://catalog.example.com/warehouse",
    "lance.catalog.bearer-token-file" = "/run/secrets/lance/catalog-token"
);

SHOW DATABASES FROM lance_rest;
SHOW TABLES FROM lance_rest.analytics;
SELECT id FROM lance_rest.analytics.events WHERE id > 10 LIMIT 20;
```

| Property | Default | Meaning |
| --- | --- | --- |
| `lance.catalog.type` | `static` | `static` uses explicit table properties; `rest` discovers tables remotely. |
| `lance.catalog.uri` | None | Required in REST mode. HTTP(S) service base URL, including any warehouse path prefix, without `/v1`. Do not embed credentials, query parameters, or fragments. |
| `lance.catalog.bearer-token-file` | Empty | Path to a file containing a Bearer token. Mount it at the same path on every FE and BE/CN. Empty means no Authorization header. |

The connector uses the standard namespace-list, namespace table-list, and table-describe
endpoints. It follows `page_token` on both listing APIs and discovers nested and empty
namespaces. A nested namespace such as `["team", "analytics"]` appears as the database
`team.analytics`; quote that database name with backticks in SQL. Literal `.`, `%`, and
`$` inside a namespace component are escaped as `%2E`, `%25`, and `%24`. The root namespace
appears as the database `$`.

```sql
SELECT e.id, count(*) AS matches
FROM lance_rest.`team.analytics`.events e
JOIN lance_rest.`team.analytics`.users u ON e.id = u.id
GROUP BY e.id;
```

The FE opens the registered dataset to obtain its Arrow schema and version. The BE reads
that version, so commits after planning do not change the dataset snapshot. FE and BE/CN
need network access to both the catalog and the dataset's object storage. The catalog
must allow listing namespaces, listing created tables, and describing a table with
`vend_credentials=true`. The connector passes returned `storage_options` to Lance,
including short-lived Azure SAS or S3 credentials. Lance's namespace credential provider
can call table-describe again to refresh expiring storage credentials. The service must
supply expiry metadata understood by Lance, such as `expires_at_millis`.

Token files are read on every catalog request, including refresh requests. Rotate the
mounted file atomically before expiry. The file contains the raw token, without the
`Bearer ` prefix. Use HTTPS outside a trusted local network. The catalog URL, table
identifier, dataset version, and file path travel in scan metadata; Bearer tokens and
vended storage credentials do not. REST mode uses a shared catalog service identity, not the SQL user's
identity. Restrict access with StarRocks catalog privileges and scope the service identity
accordingly. OAuth login, per-user token forwarding, and managed-versioning tables are
not supported by this mode.

Build FE and BE with the Lance connector enabled (the default). FE runtime libraries are
installed under `fe/lib/lance-reader-lib`, independently of the FE's Arrow dependencies.
The FE launcher enables `--add-opens=java.base/java.nio=ALL-UNNAMED`; custom Java launchers
must also supply this option. Keep FE and BE reader artifacts at the same version. REST
mode cannot be combined with static `table.*` registrations. Static catalog cloud
credentials are not used in REST mode; credentials come from the REST service or Lance's
native storage environment on each host.

Discovery fails explicitly on repeated pagination tokens, more than 10,000 pages per
listing, more than 10,000 namespaces, or namespace depth over 64. HTTP connections time
out after 10 seconds and responses after 30 seconds. HTTP redirects are not followed.

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

In static mode, SAS tokens and temporary S3 credentials are static catalog configuration; the connector does not obtain or refresh them from a remote credential service. Replace expiring credentials before starting new queries. Never place credentials in SQL predicates or dataset URI query strings.

## Query behavior and limits

- Register multiple datasets with additional `table.<name>.uri` and `table.<name>.schema` properties. They can participate in joins, aggregations, filters, and projections.
- Each table scan uses one range covering every dataset fragment. Fragment-level parallel scans and predicate pushdown into Lance are not implemented. StarRocks evaluates SQL predicates on the decoded rows.
- Column pruning reads the projected columns and columns required by predicates. `COUNT(*)` retains a scalar column when available.
- Arrow large strings and large binary values are supported. Declare them as `large_string` (or `large_utf8`) and `large_binary`. Unsigned integers widen without overflow: `uint8` → `SMALLINT`, `uint16` → `INT`, `uint32` → `BIGINT`, and `uint64` → `DECIMAL(20,0)`.
- Scalar values, dates, timestamps, and lists are supported by the reader. Arrow date64 values are interpreted as UTC dates. Map and struct materialization is not supported.
- The catalog is read-only. Local file URIs must be accessible on the BE selected for the scan.
- Reader dependencies are packaged in `be/lib/lance-reader-lib`, with the scanner factory JAR also in `be/lib/jni-packages`.

To build without Lance, pass `--without-connector-lance` to `build.sh`. This disables the BE connector, skips the reader Maven module, and excludes its JARs from the BE package.
