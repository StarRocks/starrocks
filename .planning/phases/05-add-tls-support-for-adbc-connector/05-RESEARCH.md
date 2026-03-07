# Phase 5: Add TLS Support for ADBC Connector - Research

**Researched:** 2026-03-07
**Domain:** TLS/mTLS for Arrow Flight SQL gRPC connections (Java FE + C++ BE)
**Confidence:** HIGH

## Summary

TLS support for the ADBC connector requires reading PEM certificate files from disk and passing their contents to the ADBC Flight SQL driver via driver-specific option keys. The Java driver (FE) accepts `InputStream` objects via `FlightSqlConnectionProperties` typed keys, while the C/Go driver (BE) accepts PEM content as plain strings via `AdbcDatabaseSetOption`. Both drivers use the `adbc.flight.sql.client_option.*` namespace for TLS options and require the URI scheme `grpc+tls://` to activate TLS.

The implementation touches 5 files for functional changes (ADBCConnector.java, ADBCMetadata.java, PlanNodes.thrift, ADBCScanNode.java, adbc_scanner.cpp/h) plus the ADBCDataSource/connector BE wiring. The pattern is straightforward: catalog properties store file paths, FE validates files exist at CREATE CATALOG time, FE passes file paths to BE via Thrift, both sides read file contents and pass to ADBC driver.

**Primary recommendation:** Read cert file contents at connection time (not catalog creation), pass PEM content directly to ADBC driver options. Validate file existence/readability at CREATE CATALOG time only.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- File paths only -- properties point to PEM files on disk (no inline PEM content)
- Full mTLS support -- three cert properties: CA cert, client cert, client private key
- Each node (FE and BE) reads certificate files locally -- no cert content distributed via Thrift
- Single CA cert file (can contain multiple concatenated certs), no directory support
- TLS activated by URI scheme auto-detection: `grpc+tls://` enables TLS, `grpc://` stays plaintext
- Only `grpc+tls://` recognized as TLS scheme (not `grpcs://`)
- If user provides cert properties with plaintext `grpc://` URI: log warning, ignore certs, connect without TLS
- No explicit `adbc.tls.enabled` property -- URI scheme is the sole TLS trigger
- Use `adbc.tls.*` namespace for all TLS properties:
  - `adbc.tls.ca_cert_file` -- path to CA root certificate PEM file
  - `adbc.tls.client_cert_file` -- path to client certificate PEM file (for mTLS)
  - `adbc.tls.client_key_file` -- path to client private key PEM file (for mTLS)
  - `adbc.tls.verify` -- boolean, default true. Set to false to disable cert verification
- Properties validated at CREATE CATALOG time -- fail fast if cert files don't exist or aren't readable
- `adbc.tls.verify=false` disables both certificate chain validation AND hostname matching (full insecure mode)
- Log a prominent WARNING when insecure mode is enabled
- Default (no cert properties, just `grpc+tls://` URI): use system CA bundle (Java cacerts for FE, OS CA bundle for BE C++)
- Custom CA cert file overrides system CA bundle when provided

### Claude's Discretion
- How to map `adbc.tls.*` properties to ADBC Flight SQL driver-specific options (Java and C++ APIs differ)
- Thrift field additions to TADBCScanNode for passing TLS file paths to BE
- FE validation logic for checking file existence and readability at catalog creation
- How to configure gRPC channel TLS options in BE C++ ADBC driver
- Error messages and logging format for TLS-related failures
- Test approach for TLS connections (self-signed cert generation, test fixtures)

### Deferred Ideas (OUT OF SCOPE)
- Certificate rotation without catalog restart -- requires connection pool awareness
- Per-driver TLS option customization (different drivers may use different TLS option keys)
- TLS session caching/resumption for connection-per-operation performance
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| TLS-01 | `grpc+tls://` URI scheme activates TLS; `grpc://` stays plaintext | ADBC Flight SQL driver auto-detects TLS from URI scheme |
| TLS-02 | `adbc.tls.ca_cert_file` property accepts PEM file path for CA root cert | File read at connect time, PEM content passed to driver options |
| TLS-03 | `adbc.tls.client_cert_file` + `adbc.tls.client_key_file` enable mTLS | Both must be provided together (driver enforces this) |
| TLS-04 | `adbc.tls.verify=false` disables all cert verification | Maps to `adbc.flight.sql.client_option.tls_skip_verify=true` |
| TLS-05 | CREATE CATALOG validates cert file existence and readability | FE-side validation in ADBCConnector.validate() |
| TLS-06 | Warning logged when certs provided with non-TLS URI | FE-side validation in ADBCConnector constructor |
| TLS-07 | Warning logged when insecure mode enabled | Both FE and BE log at WARNING level |
| TLS-08 | TADBCScanNode carries TLS file paths from FE to BE via Thrift | New optional fields 11-14 in TADBCScanNode |
| TLS-09 | BE reads cert files locally and passes PEM content to C ADBC driver | `AdbcDatabaseSetOption` with PEM content strings |
| TLS-10 | FE reads cert files and passes as InputStream to Java ADBC driver | `FlightSqlConnectionProperties.TLS_ROOT_CERTS.set(params, inputStream)` |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| adbc-driver-flight-sql (Java) | 0.19.0 | FE TLS connection to Flight SQL | Already in project; has TLS support via FlightSqlConnectionProperties |
| adbc_driver_flightsql (C/Go) | 1.1.0 | BE TLS connection to Flight SQL | Already in project; has TLS support via database options |

### ADBC Flight SQL Driver TLS Option Keys
| Option Key | Java Type | C Type | Purpose |
|------------|-----------|--------|---------|
| `adbc.flight.sql.client_option.tls_root_certs` | `TypedKey<InputStream>` | `const char*` (PEM content) | CA root certificate(s) |
| `adbc.flight.sql.client_option.mtls_cert_chain` | `TypedKey<InputStream>` | `const char*` (PEM content) | Client certificate for mTLS |
| `adbc.flight.sql.client_option.mtls_private_key` | `TypedKey<InputStream>` | `const char*` (PEM content) | Client private key for mTLS |
| `adbc.flight.sql.client_option.tls_skip_verify` | `TypedKey<Boolean>` | `const char*` ("true"/"false") | Disable cert verification |

### No Additional Dependencies Needed
TLS support is built into the existing ADBC Flight SQL drivers. No new Maven or CMake dependencies required.

## Architecture Patterns

### Property Flow: User -> FE -> BE

```
User: CREATE CATALOG ... PROPERTIES('adbc.tls.ca_cert_file'='/path/to/ca.pem', ...)
  |
  v
ADBCConnector (FE): Validate file exists, store in properties map
  |
  v
ADBCMetadata.initDatabase (FE): Read file -> FileInputStream -> FlightSqlConnectionProperties.TLS_ROOT_CERTS.set()
  |
  v
ADBCScanNode.toThrift (FE): Pass file paths to TADBCScanNode fields
  |
  v
ADBCScanner._init_adbc (BE): Read file content -> AdbcDatabaseSetOption("adbc.flight.sql.client_option.tls_root_certs", content)
```

### Pattern 1: FE TLS Configuration in initDatabase()

**What:** Map StarRocks `adbc.tls.*` properties to ADBC driver-specific options
**When to use:** Every time ADBCMetadata opens a database connection

```java
// In ADBCMetadata.initDatabase()
private AdbcDatabase initDatabase(Map<String, String> properties) {
    BufferAllocator allocator = new RootAllocator();
    FlightSqlDriver driver = new FlightSqlDriver(allocator);
    Map<String, Object> params = new HashMap<>();
    AdbcDriver.PARAM_URI.set(params, properties.get(ADBCConnector.PROP_URL));
    // ... existing username/password ...

    // TLS options
    String uri = properties.get(ADBCConnector.PROP_URL);
    boolean isTls = uri != null && uri.startsWith("grpc+tls://");

    if (isTls) {
        String caCertFile = properties.get("adbc.tls.ca_cert_file");
        if (caCertFile != null) {
            FlightSqlConnectionProperties.TLS_ROOT_CERTS.set(params, new FileInputStream(caCertFile));
        }

        String clientCertFile = properties.get("adbc.tls.client_cert_file");
        String clientKeyFile = properties.get("adbc.tls.client_key_file");
        if (clientCertFile != null && clientKeyFile != null) {
            FlightSqlConnectionProperties.MTLS_CERT_CHAIN.set(params, new FileInputStream(clientCertFile));
            FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.set(params, new FileInputStream(clientKeyFile));
        }

        String verify = properties.getOrDefault("adbc.tls.verify", "true");
        if ("false".equalsIgnoreCase(verify)) {
            FlightSqlConnectionProperties.TLS_SKIP_VERIFY.set(params, true);
            LOG.warn("ADBC catalog '{}': TLS certificate verification DISABLED (insecure mode)", catalogName);
        }
    }

    return driver.open(params);
}
```

### Pattern 2: BE TLS Configuration in _init_adbc()

**What:** Read PEM files and pass content to ADBC C API
**When to use:** Every time BE creates an ADBCScanner

```cpp
// In ADBCScanner::_init_adbc()
// After AdbcDatabaseSetOption for driver, uri, username, password...

if (!_ca_cert_file.empty()) {
    std::string pem_content;
    RETURN_IF_ERROR(read_file_to_string(_ca_cert_file, &pem_content));
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database,
        "adbc.flight.sql.client_option.tls_root_certs",
        pem_content.c_str(), &error), error);
}

if (!_client_cert_file.empty() && !_client_key_file.empty()) {
    std::string cert_content, key_content;
    RETURN_IF_ERROR(read_file_to_string(_client_cert_file, &cert_content));
    RETURN_IF_ERROR(read_file_to_string(_client_key_file, &key_content));
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database,
        "adbc.flight.sql.client_option.mtls_cert_chain",
        cert_content.c_str(), &error), error);
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database,
        "adbc.flight.sql.client_option.mtls_private_key",
        key_content.c_str(), &error), error);
}

if (!_tls_verify) {
    RETURN_ADBC_NOT_OK(AdbcDatabaseSetOption(&_database,
        "adbc.flight.sql.client_option.tls_skip_verify",
        "true", &error), error);
    LOG(WARNING) << "ADBC TLS certificate verification DISABLED (insecure mode)";
}
```

### Pattern 3: Thrift Field Additions

**What:** Add TLS file path fields to TADBCScanNode
**When to use:** Thrift IDL change

```thrift
struct TADBCScanNode {
  // existing fields 1-10...
  11: optional string adbc_tls_ca_cert_file
  12: optional string adbc_tls_client_cert_file
  13: optional string adbc_tls_client_key_file
  14: optional bool adbc_tls_verify
}
```

Note: File paths (not PEM content) are passed via Thrift. Each BE node reads files locally.

### Pattern 4: FE Validation at CREATE CATALOG

**What:** Validate cert file properties at catalog creation time
**When to use:** In ADBCConnector constructor

```java
// In ADBCConnector constructor, after validate(PROP_DRIVER) and validate(PROP_URL)
String uri = properties.get(PROP_URL);
boolean isTls = uri != null && uri.startsWith("grpc+tls://");

// Warn if cert properties provided with non-TLS URI
if (!isTls && hasTlsCertProperties(properties)) {
    LOG.warn("ADBC catalog '{}': TLS certificate properties provided but URI scheme is not 'grpc+tls://'."
             + " Certificates will be ignored. Use 'grpc+tls://' to enable TLS.", catalogName);
}

// Validate cert files exist and are readable
if (isTls) {
    validateCertFile(properties, "adbc.tls.ca_cert_file");
    validateCertFile(properties, "adbc.tls.client_cert_file");
    validateCertFile(properties, "adbc.tls.client_key_file");

    // mTLS requires both cert and key
    String clientCert = properties.get("adbc.tls.client_cert_file");
    String clientKey = properties.get("adbc.tls.client_key_file");
    if ((clientCert != null) != (clientKey != null)) {
        throw new StarRocksConnectorException(
            "adbc.tls.client_cert_file and adbc.tls.client_key_file must both be provided for mTLS");
    }
}
```

### Anti-Patterns to Avoid
- **Passing PEM content via Thrift:** File contents can be large; each node should read locally
- **Reading cert files at catalog creation only:** Files should be re-read at connection time for freshness (within same catalog lifetime)
- **Forgetting InputStream cleanup:** Java FileInputStream must be closed after driver.open()

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TLS gRPC configuration | Custom gRPC channel builder with TLS | ADBC driver's built-in TLS options | Driver already wraps gRPC TLS correctly |
| PEM file parsing | Custom PEM parser | Pass raw PEM content to driver | Driver/gRPC handles PEM parsing internally |
| Certificate validation | Custom X509 validation | `adbc.flight.sql.client_option.tls_root_certs` | gRPC handles chain validation |
| File reading (BE C++) | Custom file I/O | `starrocks::FileSystem` or `std::ifstream` | Standard C++ file reading is sufficient |

**Key insight:** The ADBC Flight SQL driver already handles all TLS complexity. StarRocks only needs to: (1) validate file existence, (2) read file contents, (3) pass contents to driver options.

## Common Pitfalls

### Pitfall 1: Java InputStream Lifecycle
**What goes wrong:** FileInputStream opened for cert files is not closed, or is consumed before driver.open() completes
**Why it happens:** InputStream is passed by reference; unclear when driver reads it
**How to avoid:** Use try-with-resources at the initDatabase() scope, or let driver.open() consume the streams. The FlightSqlDriver reads the stream during `open()`, so streams can be closed after `driver.open()` returns.
**Warning signs:** File descriptor leaks, "Stream closed" exceptions

### Pitfall 2: C Driver Expects PEM Content, Not File Paths
**What goes wrong:** Passing file path string like "/path/to/ca.pem" to `AdbcDatabaseSetOption` for tls_root_certs
**Why it happens:** Confusion between StarRocks property (file path) and driver option (PEM content)
**How to avoid:** Always read file content first, then pass PEM string to driver option
**Warning signs:** "failed to append certificates" error from Go driver

### Pitfall 3: URI Scheme Detection Case Sensitivity
**What goes wrong:** `grpc+TLS://` or `GRPC+TLS://` not recognized
**Why it happens:** Case-sensitive string comparison
**How to avoid:** Use case-insensitive comparison or lowercase the URI before checking
**Warning signs:** TLS not activated despite correct-looking URI

### Pitfall 4: mTLS Requires Both Cert and Key
**What goes wrong:** User provides only client_cert_file without client_key_file (or vice versa)
**Why it happens:** Incomplete mTLS configuration
**How to avoid:** Validate at CREATE CATALOG time that both are present or both absent. The ADBC driver also enforces this ("Must provide both"), but fail-fast at catalog creation is better UX.
**Warning signs:** "Must provide both" error from ADBC driver at query time

### Pitfall 5: Default TLS Without CA Cert
**What goes wrong:** User uses `grpc+tls://` without any cert properties, expecting system CA to work
**Why it happens:** This is the correct default behavior -- Java uses cacerts, Go uses system cert pool
**How to avoid:** This is NOT a pitfall -- it should work correctly. Just document that system CA is the default.
**Warning signs:** None -- this is expected behavior

### Pitfall 6: Thrift Boolean Default
**What goes wrong:** `adbc_tls_verify` field defaults to false when not set (Thrift optional bool)
**Why it happens:** Thrift optional fields have no default; `__isset` check is needed
**How to avoid:** In BE, check `__isset.adbc_tls_verify` before reading the value. Default to true (verify enabled) when field is not set.
**Warning signs:** TLS verification unexpectedly disabled

## Code Examples

### Reading PEM File in C++

```cpp
// Source: Standard C++ file I/O
static Status read_file_to_string(const std::string& path, std::string* content) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return Status::InvalidArgument(fmt::format("Cannot open certificate file: {}", path));
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    *content = ss.str();
    return Status::OK();
}
```

### Java File Validation

```java
// Source: Standard Java file validation
private void validateCertFile(Map<String, String> properties, String key) {
    String path = properties.get(key);
    if (path == null) {
        return; // Optional property
    }
    java.io.File file = new java.io.File(path);
    if (!file.exists()) {
        throw new StarRocksConnectorException(key + " file does not exist: " + path);
    }
    if (!file.isFile()) {
        throw new StarRocksConnectorException(key + " is not a regular file: " + path);
    }
    if (!file.canRead()) {
        throw new StarRocksConnectorException(key + " file is not readable: " + path);
    }
}
```

### URI Scheme Detection

```java
// Source: Project convention from CONTEXT.md
private static boolean isTlsUri(String uri) {
    return uri != null && uri.toLowerCase().startsWith("grpc+tls://");
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No TLS support | TLS via driver options | Phase 5 | Encrypted connections to Flight SQL servers |

**ADBC Driver Version Notes:**
- Java 0.19.0: FlightSqlConnectionProperties has full TLS support (TLS_ROOT_CERTS, MTLS_CERT_CHAIN, MTLS_PRIVATE_KEY, TLS_SKIP_VERIFY)
- C/Go driver (apache-arrow-adbc-19 / v1.1.0): Full TLS support via string options
- Both versions are already in the project thirdparty

## Open Questions

1. **FileSystem API for BE file reading**
   - What we know: Standard C++ `std::ifstream` works for local files
   - What's unclear: Should we use StarRocks `FileSystem` API for consistency? Or is plain ifstream simpler and sufficient?
   - Recommendation: Use plain `std::ifstream` -- cert files are always local, and FileSystem API adds unnecessary complexity

2. **ADBCScanner constructor parameter growth**
   - What we know: Constructor already has 7 parameters (driver, uri, username, password, token, sql, tuple_desc)
   - What's unclear: Adding 4 more TLS params (ca_cert_file, client_cert_file, client_key_file, tls_verify) makes it unwieldy
   - Recommendation: Consider a small struct/config object, or just add the params directly (matches existing pattern, avoids refactoring)

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 5 (FE) + Google Test (BE) |
| Config file | fe/pom.xml, be/test/CMakeLists.txt |
| Quick run command | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` |
| Full suite command | `./run-fe-ut.sh` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| TLS-05 | CREATE CATALOG validates cert file existence | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` | Exists (extend) |
| TLS-06 | Warning logged for certs with non-TLS URI | unit | `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest` | Exists (extend) |
| TLS-08 | Thrift carries TLS fields | unit | `./run-fe-ut.sh --test com.starrocks.sql.plan.ADBCScanPlanTest` | Exists (extend) |

### Sampling Rate
- **Per task commit:** `./run-fe-ut.sh --test com.starrocks.connector.adbc.ADBCConnectorTest`
- **Per wave merge:** `./run-fe-ut.sh`
- **Phase gate:** Full suite green before verify-work

### Wave 0 Gaps
None -- existing test infrastructure covers all phase requirements. Unit tests for validation logic can be added to existing ADBCConnectorTest and ADBCScanPlanTest.

## Sources

### Primary (HIGH confidence)
- ADBC Flight SQL driver Go source code (local): `thirdparty/src/arrow-adbc-apache-arrow-adbc-19/go/adbc/driver/flightsql/flightsql_database.go` -- Verified TLS option keys and that values are PEM content strings
- ADBC Flight SQL driver Go source code (local): `thirdparty/src/arrow-adbc-apache-arrow-adbc-19/go/adbc/driver/flightsql/flightsql_driver.go` -- Verified option key constants
- ADBC Java driver jar (local): `adbc-driver-flight-sql-0.19.0.jar` -- Decompiled FlightSqlConnectionProperties, verified TypedKey types (InputStream for certs, Boolean for skip_verify)
- [Flight SQL Driver - ADBC 22 documentation](https://arrow.apache.org/adbc/current/driver/flight_sql.html) -- Official TLS option documentation

### Secondary (MEDIUM confidence)
- [ADBC GitHub Issue #745](https://github.com/apache/arrow-adbc/issues/745) -- Java driver feature parity (closed/resolved)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- verified from local source code and jar decompilation
- Architecture: HIGH -- straightforward mapping of StarRocks properties to ADBC driver options
- Pitfalls: HIGH -- verified from driver source code (PEM content vs file path, mTLS pair requirement)
- TLS option keys: HIGH -- extracted from Go driver constants and Java bytecode

**Research date:** 2026-03-07
**Valid until:** 2026-04-07 (stable -- ADBC driver versions already pinned in project)
