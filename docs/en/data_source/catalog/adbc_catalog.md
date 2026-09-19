---
displayed_sidebar: docs
toc_max_heading_level: 4
description: "Use an ADBC catalog to query Arrow Flight SQL data without loading it into StarRocks."
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# ADBC catalog

<Beta />

An ADBC catalog is an external catalog that lets StarRocks query a data source through a native [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/) driver. The initial supported capability is querying an Arrow Flight SQL service.

The FE uses the ADBC driver to discover schemas, tables, and Arrow column types. The BE or CN uses the same driver to execute the pushed-down SQL and imports the returned Arrow stream through the Arrow C Data interface.

## Limitations

- The initial supported data source is Arrow Flight SQL. Other ADBC drivers are not guaranteed to work.
- ADBC catalogs are read-only.
- StarRocks maps an ADBC `db_schema` to a StarRocks database. The ADBC catalog level is not exposed.
- Each scan uses one ADBC stream. Distributed or partitioned reads are not supported.
- The following Arrow types are supported: signed and unsigned integers, floating-point values, decimal values, Boolean values, UTF-8 strings, binary values, dates, timestamps, and null values. Complex Arrow types are not supported.

Arrow Date32 (days) maps to StarRocks `DATE`; Date64 (milliseconds) maps to `DATETIME`.

## Prerequisites

### Install the native ADBC driver

StarRocks packages its own `libadbc_driver_jni` bridge. You do not need to download or replace this library when using the standard StarRocks package.

The data source driver itself is not included. Install an ADBC driver that is compatible with the StarRocks ADBC Driver Manager on every FE and every BE or CN node. Both processes load the native driver directly.

Register the driver by using the upstream ADBC Driver Manager discovery mechanism. The recommended deployment is a system driver manifest. For example, to register the Flight SQL driver with the logical name `adbc_driver_flightsql`, create `/etc/adbc/drivers/adbc_driver_flightsql.toml` on each node:

```toml
manifest_version = 1
name = "ADBC Flight SQL Driver"

[Driver]
entrypoint = "AdbcDriverInit"
shared = "/opt/adbc/lib/libadbc_driver_flightsql.so"
```

Ensure that the StarRocks service account can read the manifest and the shared library. You can also use another [ADBC driver manifest search path](https://arrow.apache.org/adbc/current/format/driver_manifests.html), such as a directory listed in `ADBC_DRIVER_PATH`, as long as both FE and BE or CN processes inherit the same discovery setup.

The catalog property `driver` is the logical manifest name without `.toml`. It is not a shared-library path. StarRocks rejects driver values that contain a path separator.

### Open Java NIO for Arrow in the FE

On Java 9 and later, add the following option to `JAVA_OPTS` in **fe.conf** on every FE node, and then restart all FEs:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

For example, preserve the existing FE options and append the option:

```properties
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED <existing_FE_JAVA_OPTS>"
```

This option is required because the FE uses Arrow Java to read ADBC metadata. It is not required by the C++ ADBC scanner on BE or CN nodes.

The standard FE package sets `adbc_jni_library_path` to `${STARROCKS_HOME}/lib/adbc_driver_jni`, where it installs the StarRocks-built JNI bridge. Change this FE configuration only if you move the packaged bridge. Do not set it to the Flight SQL driver or another data source driver.

## Create an ADBC catalog

### Syntax

```sql
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### Parameters

#### `catalog_name`

The name of the ADBC catalog. For the naming conventions, see [System limits](../../sql-reference/System_limit.md).

#### `comment`

The optional description of the catalog.

#### `PROPERTIES`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `type` | Yes | Set this value to `adbc`. |
| `driver` | Yes | Logical ADBC driver name resolved by the ADBC Driver Manager, for example `adbc_driver_flightsql`. Do not specify a file path. |
| `uri` | Yes | Data source URI accepted by the driver, for example `grpc+tls://flight.example.com:31337`. |
| `username` | No | Username passed to the ADBC driver. |
| `password` | No | Password passed to the ADBC driver. |
| `adbc.*` | No | Driver-specific ADBC options. The complete key and value are forwarded to the driver before the database is initialized. |

Properties other than the listed top-level properties must start with `adbc.`.

### Flight SQL example

The following example creates a catalog for a Flight SQL endpoint whose native driver is registered as `adbc_driver_flightsql`:

```sql
CREATE EXTERNAL CATALOG flight_sql
COMMENT "Flight SQL service"
PROPERTIES
(
    "type" = "adbc",
    "driver" = "adbc_driver_flightsql",
    "uri" = "grpc+tls://flight.example.com:31337",
    "username" = "flight_user",
    "password" = "change_me",
    "adbc.flight.sql.rpc.timeout_seconds.connect" = "10"
);
```

Use the TLS and authentication options required by your Flight SQL service. For example, the Flight SQL driver accepts options under `adbc.flight.sql.client_option.*` and `adbc.flight.sql.rpc.*`.

## Query an ADBC catalog

List the schemas exposed as StarRocks databases:

```sql
SHOW DATABASES FROM flight_sql;
```

List tables in a schema:

```sql
SHOW TABLES FROM flight_sql.main;
```

Query a table by using a fully qualified name:

```sql
SELECT n_nationkey, n_name
FROM flight_sql.main.nation
WHERE n_nationkey = 24;
```

You can also use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) and [USE](../../sql-reference/sql-statements/Database/USE.md) before querying the table.

StarRocks pushes column projection, supported predicates, and `LIMIT` to the SQL executed through the ADBC driver.

## View and drop an ADBC catalog

View all catalogs or the creation statement of a specific catalog:

```sql
SHOW CATALOGS;
SHOW CREATE CATALOG flight_sql;
```

Drop the catalog:

```sql
DROP CATALOG flight_sql;
```

Dropping the StarRocks catalog does not remove the external ADBC driver or modify the remote data source.

## Troubleshooting

### The driver cannot be loaded

If the error contains `Could not load` or `dlopen() failed`, verify the following on every FE and BE or CN node:

- The manifest filename matches `<driver>.toml`.
- The manifest is in a default ADBC search directory or a directory listed in `ADBC_DRIVER_PATH`.
- The shared-library path in the manifest exists and is readable by the StarRocks service account.
- The library matches the node operating system and CPU architecture, and all of its native dependencies can be loaded.

### The StarRocks JNI bridge cannot be found

If the FE reports that the StarRocks-built JNI bridge cannot be found, verify that the standard package contains `lib/adbc_driver_jni/libadbc_driver_jni.so` and that `adbc_jni_library_path` points to its containing directory. This setting is for the JNI bridge, not the external ADBC data source driver.

### Arrow cannot access Java NIO

If catalog metadata access fails with a Java module-access or `java.nio` reflection error, verify that every FE includes `--add-opens=java.base/java.nio=ALL-UNNAMED` in `JAVA_OPTS` and has been restarted.
