# Phase 5: Add TLS Support for ADBC Connector - Context

**Gathered:** 2026-03-07
**Status:** Ready for planning

<domain>
## Phase Boundary

Enable encrypted TLS connections between StarRocks ADBC connector and remote Arrow Flight SQL servers. Covers both FE (Java ADBC metadata operations) and BE (C++ ADBC data scanning). Includes CA root verification, mutual TLS (mTLS) with client certificates, insecure mode for dev/testing, and certificate file validation at catalog creation time.

</domain>

<decisions>
## Implementation Decisions

### Certificate Handling
- File paths only — properties point to PEM files on disk (no inline PEM content)
- Full mTLS support — three cert properties: CA cert, client cert, client private key
- Each node (FE and BE) reads certificate files locally — no cert content distributed via Thrift
- Single CA cert file (can contain multiple concatenated certs), no directory support

### URI Scheme & TLS Activation
- TLS activated by URI scheme auto-detection: `grpc+tls://` enables TLS, `grpc://` stays plaintext
- Only `grpc+tls://` recognized as TLS scheme (not `grpcs://`)
- If user provides cert properties with plaintext `grpc://` URI: log warning, ignore certs, connect without TLS
- No explicit `adbc.tls.enabled` property — URI scheme is the sole TLS trigger

### Property Naming
- Use `adbc.tls.*` namespace for all TLS properties:
  - `adbc.tls.ca_cert_file` — path to CA root certificate PEM file
  - `adbc.tls.client_cert_file` — path to client certificate PEM file (for mTLS)
  - `adbc.tls.client_key_file` — path to client private key PEM file (for mTLS)
  - `adbc.tls.verify` — boolean, default true. Set to false to disable cert verification
- Properties validated at CREATE CATALOG time — fail fast if cert files don't exist or aren't readable

### Verification Mode
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

</decisions>

<specifics>
## Specific Ideas

- "Like JDBC" pattern continues — mirror how other StarRocks connectors handle TLS where applicable
- StarRocks' own Arrow Flight SQL service already has TLS support (GlobalVariable, ArrowFlightSqlTicketManager with tls_root_certs) — reference this for consistency
- Connection-per-operation (FE) and connection-per-scan (BE) architecture unchanged — TLS config is just additional connection parameters
- Both FE Java ADBC API and BE C++ ADBC C API need TLS options passed through their respective connection initialization paths

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ArrowFlightSqlTicketManager.java`: StarRocks' own Flight SQL TLS handling — uses `tls_root_certs` gRPC option
- `GlobalVariable.java`: Has `arrow_flight_sql_tls_root_certs` pattern for TLS cert configuration
- `ADBCConnector.java`: Properties map already passed through — add TLS property constants alongside PROP_DRIVER and PROP_URL
- `ADBCMetadata.initDatabase()`: Entry point for FE TLS configuration — FlightSqlDriver params map

### Established Patterns
- FE: `AdbcDriver.PARAM_URI.set(params, ...)` for passing connection options to ADBC Java driver
- BE: `AdbcDatabaseSetOption(&_database, "key", "value", &error)` for C API options
- Catalog property validation in `ADBCConnector.validate()` — extend for cert file checks
- TADBCScanNode Thrift struct passes connection params from FE to BE (fields 6-10)

### Integration Points
- `ADBCConnector.java`: Add TLS property constants, validate cert files at creation
- `ADBCMetadata.initDatabase()`: Pass TLS options to FlightSqlDriver params
- `TADBCScanNode` (PlanNodes.thrift): Add fields for TLS file paths (ca_cert, client_cert, client_key, verify)
- `ADBCScanNode.java` (FE): Include TLS properties in toThrift()
- `ADBCScanner::_init_adbc()` (BE C++): Read cert files, set ADBC database TLS options

</code_context>

<deferred>
## Deferred Ideas

- Certificate rotation without catalog restart — requires connection pool awareness
- Per-driver TLS option customization (different drivers may use different TLS option keys)
- TLS session caching/resumption for connection-per-operation performance

</deferred>

---

*Phase: 05-add-tls-support-for-adbc-connector*
*Context gathered: 2026-03-07*
