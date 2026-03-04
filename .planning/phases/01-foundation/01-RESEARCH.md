# Phase 1: Foundation - Research

**Researched:** 2026-03-04
**Domain:** StarRocks external catalog connector infrastructure (Java FE + C++ BE build deps), Apache Arrow ADBC Java API, Arrow type mapping
**Confidence:** HIGH

## Summary

Phase 1 builds the complete ADBC catalog foundation: DDL works (CREATE/DROP/ALTER CATALOG), metadata is browsable (SHOW DATABASES, SHOW TABLES, DESCRIBE), Arrow types map to StarRocks types, and both FE and BE build cleanly with ADBC dependencies. No data scanning is involved — that is Phase 2.

The primary reference implementation is the JDBC connector at `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/`. Every class, pattern, and interface used there has a direct ADBC parallel. The code structure is well-understood from source inspection; no guesswork is required.

The FE Java ADBC library (`org.apache.arrow.adbc:adbc-core` + `adbc-driver-flight-sql`) at version **0.19.0** is confirmed compatible with the existing Arrow **18.0.0** dependency already in the project. The BE C++ ADBC library does not yet exist in the thirdparty system and must be added following the established vars.sh + build-thirdparty.sh + CMakeLists.txt pattern.

**Primary recommendation:** Mirror JDBC connector structure exactly. Every naming decision, cache pattern, and interface implementation is already proven — ADBC is a sibling connector, not a foreign one.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Mirror JDBC catalog property pattern but with `adbc.` prefix for namespace clarity
- Required properties: `type` (='adbc'), `adbc.driver` (='flight_sql'), `adbc.url` (gRPC endpoint)
- Optional properties: `adbc.username`, `adbc.password`, `adbc.token`, plus driver-specific properties passed through as-is
- Connection properties prefixed with driver name get forwarded: e.g., `flight_sql.timeout` passes through to the FlightSQL driver
- Metadata cache properties follow existing pattern: `adbc_meta_cache_enable`, `adbc_meta_cache_expire_sec`
- FE package: `com.starrocks.connector.adbc` — mirrors JDBC's `connector.jdbc` package
- Core classes follow JDBC naming pattern: ADBCConnector, ADBCMetadata, ADBCMetaCache, ADBCTable, ADBCSchemaResolver, FlightSQLSchemaResolver
- Schema resolver abstraction: base `ADBCSchemaResolver` with per-driver subclasses
- Arrow type mapping rules (int8/16/32/64 → TINYINT/SMALLINT/INT/BIGINT, uint promotion, decimal128, utf8 → VARCHAR, binary → VARBINARY, boolean, date32 → DATE, timestamp → DATETIME, null → NULL)
- Unsupported types (list, struct, map, union): log warning, exclude column from schema
- FE: Add Maven dependencies to `fe/fe-core/pom.xml`
- BE: Add C++ ADBC as thirdparty dependency
- Pin specific versions for reproducible builds
- Architecture: FE Java ADBC for metadata, BE native C++ ADBC for data scanning — no JNI for data path

### Claude's Discretion
- Exact Maven dependency versions (use latest stable compatible with existing Arrow/gRPC versions)
- Internal caching strategy details (Caffeine config, eviction policies)
- Error message wording for invalid properties
- Whether to use a factory pattern or direct instantiation for schema resolvers

### Deferred Ideas (OUT OF SCOPE)
- None — discussion stayed within phase scope
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| BUILD-01 | FE Maven adds `org.apache.arrow.adbc:adbc-core` and `adbc-driver-flight-sql` dependencies | Version compatibility confirmed: 0.19.0 targets Arrow 18.0.0, matching existing FE arrow.version |
| BUILD-02 | BE CMake adds C++ ADBC library as thirdparty dependency | Pattern confirmed: vars.sh entry + build-thirdparty.sh build_adbc() function + CMakeLists.txt linking |
| BUILD-03 | Both FE and BE build cleanly with new dependencies | CI-validated via `./build.sh --fe --be` |
| CAT-01 | User can CREATE CATALOG with type='adbc' and driver/connection properties | ConnectorType enum registration + ADBCConnector constructor |
| CAT-02 | User can DROP CATALOG to remove an ADBC catalog | No connector-side code needed; handled by DDL layer via ConnectorMgr |
| CAT-03 | User can ALTER CATALOG to update ADBC catalog properties | Connector shutdown + re-create pattern |
| CAT-04 | ADBC catalog accepts `adbc.driver` property to select driver | Property key constant in ADBCConnector, resolver factory in ADBCMetadata |
| CAT-05 | ADBC catalog accepts connection properties (uri, user, password, driver-specific options) | Property validation in ADBCConnector constructor |
| CAT-06 | User can SHOW DATABASES to list remote schemas/databases | ConnectorMetadata.listDbNames() → AdbcConnection.getObjects() |
| CAT-07 | User can SHOW TABLES to list remote tables in a database | ConnectorMetadata.listTableNames() → AdbcConnection.getObjects() |
| CAT-08 | User can DESCRIBE table to see column metadata with StarRocks types | ConnectorMetadata.getTable() → AdbcConnection.getTableSchema() → FlightSQLSchemaResolver.convertArrowToSRColumn() |
| CAT-09 | Metadata is cached with configurable TTL | ADBCMetaCache (Caffeine-based, same as JDBCMetaCache) with `adbc_meta_cache_enable` and `adbc_meta_cache_expire_sec` properties |
| TYPE-01 | Arrow types map to StarRocks types | FlightSQLSchemaResolver.convertArrowFieldToSRType() using ArrowType subclass dispatch |
| TYPE-02 | Schema resolver abstraction supports per-driver type mapping overrides | ADBCSchemaResolver abstract base, FlightSQLSchemaResolver first concrete subclass |
| TYPE-03 | FlightSQL schema resolver handles Arrow Flight SQL specific type conventions | FlightSQLSchemaResolver.convertArrowFieldToSRType() handles timestamp units/timezone, decimal128, uint promotion |
| TYPE-04 | Unsupported Arrow types are gracefully handled (logged warning, column excluded or mapped to VARCHAR) | LOG.warn() + skip column in convertToSRTable() |
</phase_requirements>

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `org.apache.arrow.adbc:adbc-core` | 0.19.0 | ADBC Java API interfaces (AdbcDatabase, AdbcConnection, AdbcStatement) | Official Apache Arrow ADBC Java API; 0.19.0 pins to Arrow 18.0.0 matching existing project |
| `org.apache.arrow.adbc:adbc-driver-flight-sql` | 0.19.0 | FlightSqlDriver implementation | Official driver; same Arrow 18.0.0 dependency; includes FlightSqlDriver class |
| `com.github.ben-manes.caffeine:caffeine` | (already in project) | Metadata caching | Already used by JDBCMetaCache; exact same pattern |
| Arrow C++ ADBC | 1.1.0 (C API) | BE native C++ ADBC for Phase 2 data path | Added now to validate build; actual use in Phase 2 |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `org.apache.arrow:arrow-vector` | 18.0.0 (already present) | ArrowType classes for type mapping | Already in project; needed for `org.apache.arrow.vector.types.pojo.ArrowType` |
| `org.apache.arrow:flight-core` | 18.0.0 (already present) | Flight protocol support | Already in project; FlightSqlDriver depends on it |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `adbc-driver-flight-sql` | Raw `flight-sql` JDBC driver (already in project) | ADBC provides cleaner metadata API via getObjects/getTableSchema vs JDBC ResultSet |
| ADBC 0.19.0 | ADBC 0.15.0–0.18.0 | All target Arrow 18.0.0; 0.19.0 is latest in the 18.x-compatible series |

**Installation (FE Maven — add to `fe/fe-core/pom.xml` `<dependencies>` section):**
```xml
<dependency>
    <groupId>org.apache.arrow.adbc</groupId>
    <artifactId>adbc-core</artifactId>
    <version>${adbc.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow.adbc</groupId>
    <artifactId>adbc-driver-flight-sql</artifactId>
    <version>${adbc.version}</version>
</dependency>
```

Add to `fe/pom.xml` `<properties>`:
```xml
<adbc.version>0.19.0</adbc.version>
```

---

## Architecture Patterns

### Recommended Project Structure

```
fe/fe-core/src/main/java/com/starrocks/
├── catalog/
│   └── ADBCTable.java              # Table representation (extends Table)
├── connector/
│   └── adbc/
│       ├── ADBCConnector.java      # implements Connector
│       ├── ADBCMetadata.java       # implements ConnectorMetadata
│       ├── ADBCMetaCache.java      # Caffeine-based cache
│       ├── ADBCTableName.java      # Key type for caches (analogous to JDBCTableName)
│       ├── ADBCSchemaResolver.java # Abstract base: Arrow Field → Column
│       └── FlightSQLSchemaResolver.java  # First concrete resolver

fe/fe-core/src/test/java/com/starrocks/
└── connector/adbc/
    ├── ADBCConnectorTest.java
    ├── ADBCMetadataTest.java
    ├── ADBCMetaCacheTest.java
    └── FlightSQLSchemaResolverTest.java

thirdparty/
├── vars.sh              # Add ADBC_DOWNLOAD, ADBC_NAME, ADBC_SOURCE
└── build-thirdparty.sh  # Add build_adbc() function

be/CMakeLists.txt        # Add adbc to STARROCKS_DEPENDENCIES
```

### Pattern 1: ConnectorType Registration

**What:** ADBC is registered as a new enum entry in `ConnectorType.java`. This is the single source of truth that makes CREATE CATALOG TYPE='adbc' work.

**When to use:** Required once; this unlocks all DDL.

**Example:**
```java
// Source: ConnectorType.java (existing pattern)
ADBC("adbc", ADBCConnector.class, null),

// In SUPPORT_TYPE_SET:
public static final Set<ConnectorType> SUPPORT_TYPE_SET = EnumSet.of(
    ES, HIVE, ICEBERG, JDBC, HUDI, DELTALAKE, PAIMON, ODPS, KUDU, UNIFIED, BENCHMARK,
    ADBC  // new entry
);
```

### Pattern 2: ADBCConnector (mirrors JDBCConnector exactly)

**What:** Validates required properties, lazily creates ADBCMetadata.

**When to use:** Entry point for ConnectorFactory.createRealConnector().

**Example:**
```java
// Source: JDBCConnector.java pattern
public class ADBCConnector implements Connector {
    private static final String PROP_DRIVER = "adbc.driver";
    private static final String PROP_URL    = "adbc.url";

    public ADBCConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
        this.properties  = context.getProperties();
        validate(PROP_DRIVER);
        validate(PROP_URL);
        // No checksum: ADBC drivers are Maven artifacts, not downloaded JARs
        try {
            metadata = new ADBCMetadata(properties, catalogName);
        } catch (Exception e) {
            metadata = null;
            LOG.error("Failed to create ADBC metadata on [catalog: {}]", catalogName, e);
        }
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            metadata = new ADBCMetadata(properties, catalogName);
        }
        return metadata;
    }

    @Override
    public void shutdown() {
        if (metadata != null) { metadata.shutdown(); }
    }
}
```

### Pattern 3: ADBCMetadata Connection and Metadata Operations

**What:** Opens ADBC connection using FlightSqlDriver, delegates metadata operations to schema resolver.

**Key difference from JDBC:** No HikariCP connection pool — ADBC manages connections differently. AdbcDatabase + AdbcConnection lifecycle is managed explicitly.

**Example:**
```java
// Source: ADBC Java API (arrow.apache.org/adbc)
import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.RootAllocator;

public class ADBCMetadata implements ConnectorMetadata {

    private final AdbcDatabase adbcDatabase;

    public ADBCMetadata(Map<String, String> props, String catalogName) {
        BufferAllocator allocator = new RootAllocator();
        FlightSqlDriver driver = new FlightSqlDriver(allocator);

        Map<String, Object> params = new HashMap<>();
        AdbcDriver.PARAM_URI.set(params, props.get("adbc.url"));
        if (props.containsKey("adbc.username")) {
            AdbcDriver.PARAM_USERNAME.set(params, props.get("adbc.username"));
        }
        if (props.containsKey("adbc.password")) {
            AdbcDriver.PARAM_PASSWORD.set(params, props.get("adbc.password"));
        }

        this.adbcDatabase = driver.open(params);
        this.schemaResolver = createSchemaResolver(props.get("adbc.driver"));
        createCacheInstances(props);
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        try (AdbcConnection conn = adbcDatabase.connect();
             ArrowReader reader = conn.getObjects(
                     AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
                     null, null, null, null, null)) {
            return schemaResolver.listSchemas(reader);
        } catch (AdbcException e) {
            throw new StarRocksConnectorException("Failed to list databases", e);
        }
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return tableInstanceCache.get(new ADBCTableName(dbName, tblName), k -> {
            try (AdbcConnection conn = adbcDatabase.connect()) {
                Schema arrowSchema = conn.getTableSchema(null, dbName, tblName);
                List<Column> fullSchema = schemaResolver.convertToSRTable(arrowSchema);
                if (fullSchema.isEmpty()) return null;
                int tableId = tableIdCache.getPersistentCache(k,
                        j -> ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt());
                return new ADBCTable(tableId, tblName, fullSchema, dbName, catalogName, properties);
            } catch (AdbcException e) {
                LOG.warn("Failed to get table {}.{}", dbName, tblName, e);
                return null;
            }
        });
    }
}
```

### Pattern 4: ADBCSchemaResolver — Arrow Type Mapping

**What:** The inverse of the existing `ArrowUtils.convertToArrowType()` method (StarRocks→Arrow). The resolver does Arrow→StarRocks direction. The existing `ArrowUtils.java` provides a verified type table.

**When to use:** Called from `convertToSRTable(Schema arrowSchema)` for each field.

**Example:**
```java
// Source: Derived from ArrowUtils.java (existing, inverse direction)
// ArrowUtils.java is the authoritative source at:
// fe/fe-core/src/main/java/com/starrocks/service/arrow/flight/sql/ArrowUtils.java

public class FlightSQLSchemaResolver extends ADBCSchemaResolver {

    @Override
    public Type convertArrowFieldToSRType(Field field) {
        ArrowType arrowType = field.getType();

        if (arrowType instanceof ArrowType.Bool) {
            return Type.BOOLEAN;
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            boolean signed = intType.getIsSigned();
            int bitWidth = intType.getBitWidth();
            if (signed) {
                switch (bitWidth) {
                    case 8:  return Type.TINYINT;
                    case 16: return Type.SMALLINT;
                    case 32: return Type.INT;
                    case 64: return Type.BIGINT;
                }
            } else {
                // Unsigned: promote to avoid overflow
                switch (bitWidth) {
                    case 8:  return Type.SMALLINT;
                    case 16: return Type.INT;
                    case 32: return Type.BIGINT;
                    case 64: return ScalarType.createType(PrimitiveType.LARGEINT);
                }
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision prec = ((ArrowType.FloatingPoint) arrowType).getPrecision();
            return prec == FloatingPointPrecision.SINGLE ? Type.FLOAT : Type.DOUBLE;
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal dec = (ArrowType.Decimal) arrowType;
            return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128,
                    dec.getPrecision(), dec.getScale());
        } else if (arrowType instanceof ArrowType.Utf8
                || arrowType instanceof ArrowType.LargeUtf8) {
            return ScalarType.createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
        } else if (arrowType instanceof ArrowType.Binary
                || arrowType instanceof ArrowType.LargeBinary) {
            return Type.VARBINARY;
        } else if (arrowType instanceof ArrowType.Date) {
            return Type.DATE;
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return Type.DATETIME;
        } else if (arrowType instanceof ArrowType.Null) {
            return Type.NULL;
        }

        // Unsupported: List, Struct, Map, Union, etc.
        LOG.warn("Unsupported Arrow type {} for column {}, excluding from schema",
                arrowType, field.getName());
        return null;  // caller must skip null-typed columns
    }
}
```

### Pattern 5: ADBCMetaCache (identical to JDBCMetaCache)

**What:** Caffeine-based cache with configurable TTL, property-driven enable/disable.

**When to use:** Wrap all AdbcDatabase metadata calls. Cache lifetime is controlled by `adbc_meta_cache_enable` and `adbc_meta_cache_expire_sec` properties.

**Example:**
```java
// Source: JDBCMetaCache.java — ADBC version is identical with renamed property keys
// fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCMetaCache.java

private final String adbcMetaCacheEnable  = "adbc_meta_cache_enable";
private final String adbcMetaCacheExpireSec = "adbc_meta_cache_expire_sec";
// All other logic is identical to JDBCMetaCache
```

### Pattern 6: BE Thirdparty Addition (ADBC C++ Library)

**What:** C++ ADBC library added as a thirdparty dependency using the established vars.sh + build-thirdparty.sh pattern.

**Phase 1 scope:** Validate the build dep compiles. Actual symbol usage is Phase 2.

**Example (vars.sh additions):**
```bash
# ADBC C++ library (Apache Arrow ADBC)
ADBC_DOWNLOAD="https://github.com/apache/arrow-adbc/archive/refs/tags/apache-adbc-1.1.0.tar.gz"
ADBC_NAME="arrow-adbc-apache-adbc-1.1.0.tar.gz"
ADBC_SOURCE="arrow-adbc-apache-adbc-1.1.0"
ADBC_MD5SUM="<compute after download>"
```

**Example (build-thirdparty.sh):**
```bash
build_adbc() {
    check_if_source_exist $ADBC_SOURCE
    cd $TP_SOURCE_DIR/$ADBC_SOURCE/c
    mkdir -p build && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    ${CMAKE_CMD} \
        -DADBC_DRIVER_FLIGHTSQL=ON \
        -DADBC_BUILD_SHARED=OFF \
        -DADBC_BUILD_STATIC=ON \
        -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR \
        -DCMAKE_PREFIX_PATH=${TP_INSTALL_DIR} \
        -G "${CMAKE_GENERATOR}" ..
    ${BUILD_SYSTEM} -j$PARALLEL
    ${BUILD_SYSTEM} install
}
```

**Example (CMakeLists.txt addition to STARROCKS_DEPENDENCIES):**
```cmake
adbc
adbc_driver_flightsql
```

### Anti-Patterns to Avoid

- **Don't skip ADBCTable:** JDBCTable shows that a dedicated Table subclass is needed to hold catalog/db/properties and implement `toThrift()`. ADBCTable must do the same (even if toThrift returns a stub in Phase 1, since data scanning is Phase 2).
- **Don't hold one global AdbcConnection:** ADBC connections are not thread-safe. Open a new connection per metadata operation (or use a connection-per-thread pattern). The JDBC pattern uses HikariCP for this; ADBC should open+close connections within each metadata method.
- **Don't skip the `Table.TableType.ADBC` entry:** Multiple places in the codebase switch on `TableType`. Missing this entry causes NullPointerExceptions in AstToStringBuilder and RelationTransformer.
- **Don't hardcode "flight_sql" assumptions in base class:** The `adbc.driver` property selects the resolver. All FlightSQL specifics live only in FlightSQLSchemaResolver, not ADBCSchemaResolver or ADBCMetadata.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Metadata caching | Custom HashMap cache | `ADBCMetaCache` (copy of JDBCMetaCache with renamed property keys) | TTL expiry, thread safety, and cache invalidation all solved |
| Arrow type dispatch | Custom instanceof chain from scratch | Use `ArrowUtils.java` as the reference (inverse direction) | Existing code is authoritative and CI-tested |
| Connection parameter passing | Custom property forwarding | `AdbcDriver.PARAM_URI.set()`, `PARAM_USERNAME.set()`, `PARAM_PASSWORD.set()` | Official ADBC API constants — safe across versions |
| Schema listing | Raw FlightSQL RPC calls | `AdbcConnection.getObjects()` with `GetObjectsDepth.DB_SCHEMAS` | ADBC abstracts Flight protocol details |
| Column schema | Manual Flight SQL proto parsing | `AdbcConnection.getTableSchema(catalog, dbSchema, tableName)` returns `org.apache.arrow.vector.types.pojo.Schema` directly | Clean, driver-agnostic API |

**Key insight:** The ADBC Java API (`getObjects`, `getTableSchema`) does exactly what JDBC DatabaseMetaData does — it is the correct abstraction level. Do not drop below it to raw Flight RPC.

---

## Common Pitfalls

### Pitfall 1: Arrow Version Mismatch Between FE and ADBC

**What goes wrong:** Adding ADBC at a version that requires a different Arrow version than the project's `arrow.version=18.0.0` causes dependency hell (duplicate classes, NoClassDefFoundError at runtime).

**Why it happens:** ADBC 0.20.0+ targets Arrow 20.0.0+. Only ADBC 0.15.0–0.19.0 are confirmed to pin `dep.arrow.version=18.0.0`.

**How to avoid:** Use ADBC 0.19.0 (latest Arrow-18-compatible version). Verified via Maven POM inspection: `curl -s "https://repo1.maven.org/maven2/org/apache/arrow/adbc/arrow-adbc-java-root/0.19.0/arrow-adbc-java-root-0.19.0.pom" | grep dep.arrow.version` returns `18.0.0`.

**Warning signs:** Build errors referencing Arrow class version conflicts; mvn dependency:tree showing two arrow versions.

### Pitfall 2: AdbcConnection Thread Safety

**What goes wrong:** Sharing one `AdbcConnection` across threads causes race conditions or crashes. Unlike JDBC's HikariCP pooling, ADBC connections are not safe for concurrent use.

**Why it happens:** Each metadata method (listDbNames, listTableNames, getTable) may be called from different threads.

**How to avoid:** Open a new `AdbcConnection` within each metadata method using `adbcDatabase.connect()`, then close it in the method's try-with-resources block. `AdbcDatabase` is safe to share; `AdbcConnection` is not.

**Warning signs:** Intermittent metadata failures under concurrent SHOW TABLES calls.

### Pitfall 3: Missing Table.TableType.ADBC Causes NullPointerExceptions

**What goes wrong:** `AstToStringBuilder`, `MaterializedViewAnalyzer`, and `RelationTransformer` all switch on `Table.TableType`. Without an ADBC entry, ADBC tables cause NPE or incorrect behavior in SQL analysis.

**Why it happens:** These files use `table.getType() == Table.TableType.JDBC` guards. ADBC tables need similar guards.

**How to avoid:** Add `@SerializedName("ADBC") ADBC` to `Table.TableType` enum. Then grep for all `TableType.JDBC` references and add parallel `|| tableType == Table.TableType.ADBC` guards where appropriate for Phase 1.

**Warning signs:** `DESCRIBE my_adbc.db.table` produces a NullPointerException in FE logs.

### Pitfall 4: ADBC C++ Library Not Finding Arrow Headers

**What goes wrong:** BE build fails because the ADBC C++ cmake cannot find the Arrow headers/libraries already installed in the thirdparty.

**Why it happens:** ADBC C++ cmake needs `-DCMAKE_PREFIX_PATH=${TP_INSTALL_DIR}` to locate the pre-built Arrow.

**How to avoid:** In the `build_adbc()` function, pass `-DCMAKE_PREFIX_PATH=${TP_INSTALL_DIR}` and order ADBC to build after ARROW in the build-thirdparty.sh build order list.

**Warning signs:** cmake error `Could not find ArrowFlight` during ADBC build.

### Pitfall 5: `getTableSchema()` Null Catalog/Schema Parameters

**What goes wrong:** Passing non-null catalog name to `AdbcConnection.getTableSchema(catalog, dbSchema, tableName)` may fail on some FlightSQL servers that do not expose a catalog layer.

**Why it happens:** Arrow Flight SQL distinguishes catalog (top level) and schema (database). StarRocks's "database" corresponds to FlightSQL's "schema", and catalog should be `null`.

**How to avoid:** Always pass `null` for catalog in `getTableSchema(null, dbName, tblName)`. This matches how JDBC uses schema-level operations for metadata.

**Warning signs:** `AdbcException: catalog not found` when catalog parameter is set.

---

## Code Examples

Verified patterns from official sources and codebase inspection:

### ADBC Connection Lifecycle

```java
// Source: ADBC Java API, confirmed at arrow.apache.org/adbc/current/java/api
// Pattern from Doris FlightAdbcDriver example (verified)
import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.flight.Location;

BufferAllocator allocator = new RootAllocator();
FlightSqlDriver driver = new FlightSqlDriver(allocator);

Map<String, Object> params = new HashMap<>();
// URI format: grpc://host:port or grpc+tls://host:port
AdbcDriver.PARAM_URI.set(params,
    Location.forGrpcInsecure("localhost", 32010).getUri().toString());
AdbcDriver.PARAM_USERNAME.set(params, "user");
AdbcDriver.PARAM_PASSWORD.set(params, "pass");

try (AdbcDatabase db = driver.open(params);
     AdbcConnection conn = db.connect()) {
    // Use conn for metadata operations
}
```

### Listing Schemas (SHOW DATABASES)

```java
// Source: AdbcConnection.getObjects() - ADBC Java API
// GetObjectsDepth.DB_SCHEMAS returns catalog+schema structure
try (AdbcConnection conn = adbcDatabase.connect();
     ArrowReader reader = conn.getObjects(
             AdbcConnection.GetObjectsDepth.DB_SCHEMAS,
             null,   // catalogPattern: null = all
             null,   // dbSchemaPattern: null = all
             null,   // tableNamePattern: not used at this depth
             null,   // tableTypes
             null    // columnNamePattern
     )) {
    // reader contains Arrow data with catalog_name, db_schema_name columns
    // Parse with schema resolver
}
```

### Getting Table Column Schema (DESCRIBE)

```java
// Source: AdbcConnection.getTableSchema() - ADBC Java API
// Returns org.apache.arrow.vector.types.pojo.Schema directly
import org.apache.arrow.vector.types.pojo.Schema;

try (AdbcConnection conn = adbcDatabase.connect()) {
    Schema arrowSchema = conn.getTableSchema(
            null,     // catalog: always null for Flight SQL schema-level access
            dbName,   // dbSchema: StarRocks "database" = FlightSQL "schema"
            tblName   // tableName
    );
    List<Column> srColumns = schemaResolver.convertToSRTable(arrowSchema);
}
```

### Arrow Type to StarRocks Type (reverse of ArrowUtils.java)

```java
// Source: Inverse of ArrowUtils.convertToScalarArrowType() at
// fe/fe-core/src/main/java/com/starrocks/service/arrow/flight/sql/ArrowUtils.java
// ArrowUtils is the ground truth for type correspondences.

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.FloatingPointPrecision;

// ArrowType.Int(8,  signed=true)  → TINYINT
// ArrowType.Int(16, signed=true)  → SMALLINT
// ArrowType.Int(32, signed=true)  → INT
// ArrowType.Int(64, signed=true)  → BIGINT
// ArrowType.Int(8,  signed=false) → SMALLINT (uint8 promoted)
// ArrowType.Int(16, signed=false) → INT      (uint16 promoted)
// ArrowType.Int(32, signed=false) → BIGINT   (uint32 promoted)
// ArrowType.Int(64, signed=false) → LARGEINT (uint64 promoted)
// ArrowType.FloatingPoint(SINGLE) → FLOAT
// ArrowType.FloatingPoint(DOUBLE) → DOUBLE
// ArrowType.Decimal(p,s,128)      → DECIMAL(p,s)
// ArrowType.Utf8()                → VARCHAR(DEFAULT_STRING_LENGTH)
// ArrowType.LargeUtf8()           → VARCHAR(DEFAULT_STRING_LENGTH)
// ArrowType.Binary()              → VARBINARY
// ArrowType.LargeBinary()         → VARBINARY
// ArrowType.Bool()                → BOOLEAN
// ArrowType.Date(DAY)             → DATE
// ArrowType.Timestamp(any unit)   → DATETIME (normalize to MICROSECOND in ArrowUtils convention)
// ArrowType.Null()                → NULL type
// ArrowType.List/Struct/Map/Union → LOG.warn + skip column
```

### ConnectorType Registration

```java
// Source: ConnectorType.java
// fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java
// Add after JDBC entry:
ADBC("adbc", ADBCConnector.class, null),

// Add to SUPPORT_TYPE_SET:
ADBC,
```

### Table.TableType Addition

```java
// Source: Table.java
// fe/fe-core/src/main/java/com/starrocks/catalog/Table.java
// Add after JDBC:
@SerializedName("ADBC")
ADBC,
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| JNI bridge for external data | Native C++ ADBC in BE (Phase 2) | New for ADBC | No JNI overhead in data path |
| JDBC driver URL for external connections | ADBC driver property + gRPC URI | New for ADBC | Cleaner abstraction, Arrow-native |

**Deprecated/outdated:**
- `flight-sql-jdbc-driver` (already in project as test-scope): This is the JDBC-over-FlightSQL approach. ADBC supersedes this for metadata; do not use it in ADBC connector code.

---

## Open Questions

1. **C++ ADBC version pinning and MD5**
   - What we know: Arrow ADBC C API version 1.1.0 is latest stable (Sep 2025 release). Source at `https://github.com/apache/arrow-adbc/archive/refs/tags/apache-adbc-1.1.0.tar.gz`
   - What's unclear: Exact MD5 of the source tarball (needed for vars.sh `ADBC_MD5SUM`)
   - Recommendation: Compute MD5 during implementation via `md5sum` after download. Use the computed value.

2. **ADBC C++ cmake integration complexity**
   - What we know: ADBC C++ uses cmake and depends on Arrow C++ (already built). The cmake build needs `CMAKE_PREFIX_PATH` pointing to TP_INSTALL_DIR.
   - What's unclear: Whether ADBC C++ cmake will find all required Arrow sub-libraries (Arrow Flight SQL specifically) correctly via cmake discovery.
   - Recommendation: If cmake discovery fails, fall back to providing explicit `-DArrow_DIR`, `-DArrowFlight_DIR`, `-DArrowFlightSQL_DIR` paths pointing into `$TP_INSTALL_DIR`. This is the pattern used by other complex thirdparty builds.

3. **Connection per-operation vs connection pool for ADBC**
   - What we know: AdbcDatabase is thread-safe; AdbcConnection is not. FlightSQL connections are lightweight gRPC streams.
   - What's unclear: Whether opening a new AdbcConnection per metadata operation is too slow for production.
   - Recommendation: For Phase 1, open per operation (simplest, correct). Add connection pool or reuse in Phase 2 if benchmarking shows overhead.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 5 (Jupiter) with Mockito/JMockit |
| Config file | None — Maven surefire auto-discovers `**/*Test.java` |
| Quick run command | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` |
| Full suite command | `./run-fe-ut.sh` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BUILD-01 | FE Maven compiles with ADBC deps | build | `cd fe && mvn compile -pl fe-core -am -DskipTests` | ❌ Wave 0 (add deps first) |
| BUILD-02 | BE CMakeLists.txt links ADBC | build | `./build.sh --be` | ❌ Wave 0 (add thirdparty first) |
| BUILD-03 | Full build passes | build | `./build.sh --fe --be` | ❌ Wave 0 |
| CAT-01 | ADBCConnector validates required properties | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` | ❌ Wave 0 |
| CAT-02 | ConnectorType.ADBC in SUPPORT_TYPE_SET | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` | ❌ Wave 0 |
| CAT-04 | adbc.driver property selects resolver | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetadataTest` | ❌ Wave 0 |
| CAT-05 | Missing required properties throw exception | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` | ❌ Wave 0 |
| CAT-06 | listDbNames() parses getObjects() result | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetadataTest` | ❌ Wave 0 |
| CAT-07 | listTableNames() parses getObjects() result | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetadataTest` | ❌ Wave 0 |
| CAT-08 | getTable() converts Arrow Schema to Columns | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetadataTest` | ❌ Wave 0 |
| CAT-09 | ADBCMetaCache respects TTL and enable flag | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCMetaCacheTest` | ❌ Wave 0 |
| TYPE-01 | All supported Arrow types map correctly | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.FlightSQLSchemaResolverTest` | ❌ Wave 0 |
| TYPE-02 | Different drivers can override type mapping | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.FlightSQLSchemaResolverTest` | ❌ Wave 0 |
| TYPE-03 | Timestamp units/timezone handled | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.FlightSQLSchemaResolverTest` | ❌ Wave 0 |
| TYPE-04 | Unsupported types log warning, column excluded | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.FlightSQLSchemaResolverTest` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `cd fe && mvn compile -pl fe-core -am -DskipTests`
- **Per wave merge:** `./run-fe-ut.sh --test com.starrocks.connector.adbc`
- **Phase gate:** Full FE + BE build clean before Phase 2

### Wave 0 Gaps

- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCConnectorTest.java` — covers CAT-01, CAT-02, CAT-05
- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetadataTest.java` — covers CAT-04, CAT-06, CAT-07, CAT-08 (uses JMockit to mock AdbcDatabase)
- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/ADBCMetaCacheTest.java` — covers CAT-09
- [ ] `fe/fe-core/src/test/java/com/starrocks/connector/adbc/FlightSQLSchemaResolverTest.java` — covers TYPE-01, TYPE-02, TYPE-03, TYPE-04
- [ ] Maven dependency additions in `fe/pom.xml` and `fe/fe-core/pom.xml` — prerequisite for BUILD-01
- [ ] `thirdparty/vars.sh` ADBC entry + `build-thirdparty.sh` `build_adbc()` — prerequisite for BUILD-02

---

## Sources

### Primary (HIGH confidence)
- `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCConnector.java` — Reference implementation inspected directly
- `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCMetadata.java` — Full metadata pattern
- `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCSchemaResolver.java` — Base resolver pattern
- `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/JDBCMetaCache.java` — Caffeine cache pattern
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java` — Registration pattern
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorFactory.java` — Instantiation pattern
- `fe/fe-core/src/main/java/com/starrocks/catalog/Table.java` — TableType enum location
- `fe/fe-core/src/main/java/com/starrocks/service/arrow/flight/sql/ArrowUtils.java` — Authoritative Arrow↔StarRocks type map
- `fe/pom.xml` — arrow.version=18.0.0, grpc.version=1.63.0 confirmed
- `thirdparty/vars.sh` — BE C++ Arrow 19.0.1 confirmed; ADBC not yet present
- `thirdparty/build-thirdparty.sh` — build_arrow() pattern as template for build_adbc()
- `be/CMakeLists.txt` — arrow, arrow_flight, arrow_flight_sql already in STARROCKS_DEPENDENCIES
- Maven POM verification: `curl` on `arrow-adbc-java-root-0.19.0.pom` confirms `dep.arrow.version=18.0.0`

### Secondary (MEDIUM confidence)
- [ADBC Java API docs - AdbcConnection methods](https://arrow.apache.org/adbc/main/java/api/org/apache/arrow/adbc/core/AdbcConnection.html) — getObjects, getTableSchema, getInfo signatures verified
- [ADBC all classes index](https://arrow.apache.org/adbc/current/java/api/allclasses-index.html) — FlightSqlDriver class location confirmed
- [Doris FlightAdbcDriver example](https://github.com/apache/doris/blob/master/samples/arrow-flight-sql/java/src/main/java/doris/arrowflight/demo/FlightAdbcDriver.java) — FlightSqlDriver instantiation pattern verified
- [Maven Central adbc-driver-flight-sql versions](https://central.sonatype.com/artifact/org.apache.arrow.adbc/adbc-driver-flight-sql/versions) — 0.22.0 is latest; 0.19.0 confirmed as latest Arrow-18-compatible

### Tertiary (LOW confidence)
- [ADBC Flight SQL driver docs](https://arrow.apache.org/adbc/current/driver/flight_sql.html) — Java support noted as incomplete; some options not available vs C driver; flagged for validation during implementation

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — Maven POM versions verified by direct HTTP inspection of Maven Central
- Architecture: HIGH — All patterns taken directly from JDBC connector source code
- Type mapping: HIGH — ArrowUtils.java in the same codebase is the authoritative reference
- BE thirdparty pattern: HIGH — vars.sh + build-thirdparty.sh inspected directly
- ADBC Java metadata API: MEDIUM — API signatures verified from official docs; actual behavior with specific FlightSQL servers needs runtime validation

**Research date:** 2026-03-04
**Valid until:** 2026-04-04 (30 days; Arrow and ADBC releases are infrequent)
