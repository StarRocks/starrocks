# Requirements: OpenSearch Connector v1.0

**Defined:** 2026-03-18  
**Core Value:** Users can query OpenSearch 2.9.x indices through StarRocks SQL with appropriate security levels.

## Stage 1: HTTP Basic Connector

### Basic Connectivity (CONN)

- [ ] **CONN-01**: User can create OpenSearch catalog with host configuration
- [ ] **CONN-02**: System connects via HTTP protocol only (no HTTPS)
- [ ] **CONN-03**: System requires no authentication
- [ ] **CONN-04**: System validates connectivity on catalog creation
- [ ] **CONN-05**: System targets OpenSearch 2.9.x specifically

### Schema Discovery (SCHEMA)

- [ ] **SCHEMA-01**: System lists all non-system OpenSearch indices as tables
- [ ] **SCHEMA-02**: System treats index aliases as tables
- [ ] **SCHEMA-03**: System converts OpenSearch mapping to StarRocks schema
- [ ] **SCHEMA-04**: System infers appropriate StarRocks types from OpenSearch field types

### Query Capabilities (QUERY)

- [ ] **QUERY-01**: User can execute SELECT queries on OpenSearch indices
- [ ] **QUERY-02**: System pushes column projection to OpenSearch
- [ ] **QUERY-03**: System handles basic data types (string, numeric, boolean, date)
- [ ] **QUERY-04**: System provides meaningful error messages for query failures

---

## Stage 2: HTTPS (One-way TLS)

### HTTPS Support (HTTPS)

- [ ] **HTTPS-01**: System supports HTTPS protocol
- [ ] **HTTPS-02**: System validates server certificate against system CA store
- [ ] **HTTPS-03**: System supports custom CA certificate for server validation

### Hostname Verification (HOST)

- [ ] **HOST-01**: Hostname verification is enabled by default for HTTPS
- [ ] **HOST-02**: System verifies server hostname matches certificate SAN/CN
- [ ] **HOST-03**: User can optionally disable hostname verification

### Security (SEC)

- [ ] **SEC-01**: System validates CA certificate file exists and is readable
- [ ] **SEC-02**: System provides meaningful error messages for SSL/TLS failures

---

## Stage 3: mTLS (Mutual TLS)

### mTLS Authentication (MTLS)

- [ ] **MTLS-01**: User can configure client certificate path
- [ ] **MTLS-02**: User can configure client private key path
- [ ] **MTLS-03**: User can configure private key password (for encrypted keys)
- [ ] **MTLS-04**: System performs SSL handshake with mutual authentication
- [ ] **MTLS-05**: System validates client certificate and key pair

### Certificate Format Support (CERT)

- [ ] **CERT-01**: System supports PEM format certificates and keys
- [ ] **CERT-02**: System supports JKS (Java KeyStore) format
- [ ] **CERT-03**: System supports PKCS12 format
- [ ] **CERT-04**: System auto-detects certificate format

### Security Hardening (SECH)

- [ ] **SECH-01**: System validates private key file permissions (not world-readable)
- [ ] **SECH-02**: System supports encrypted private keys with password
- [ ] **SECH-03**: System logs certificate expiration warnings

---

## Out of Scope

| Feature | Reason |
|---------|--------|
| HTTPS in Stage 1 | Deferred to Stage 2 |
| mTLS in Stage 1/2 | Deferred to Stage 3 |
| Authentication (username/password) | Not required for target use case |
| Elasticsearch compatibility | OpenSearch 2.9.x only |
| Write operations | Focus on analytics read workload |
| OpenSearch DSL passthrough | Maintain SQL abstraction |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CONN-01 | Phase 1 | Pending |
| CONN-02 | Phase 1 | Pending |
| CONN-03 | Phase 1 | Pending |
| CONN-04 | Phase 1 | Pending |
| CONN-05 | Phase 1 | Pending |
| SCHEMA-01 | Phase 1 | Pending |
| SCHEMA-02 | Phase 1 | Pending |
| SCHEMA-03 | Phase 1 | Pending |
| SCHEMA-04 | Phase 1 | Pending |
| QUERY-01 | Phase 1 | Pending |
| QUERY-02 | Phase 1 | Pending |
| QUERY-03 | Phase 1 | Pending |
| QUERY-04 | Phase 1 | Pending |
| HTTPS-01~03 | Phase 2 | Pending |
| HOST-01~03 | Phase 2 | Pending |
| SEC-01~02 | Phase 2 | Pending |
| MTLS-01~05 | Phase 3 | Pending |
| CERT-01~04 | Phase 3 | Pending |
| SECH-01~03 | Phase 3 | Pending |

**Coverage:**
- Total requirements: 28
- Stage 1 (HTTP): 13
- Stage 2 (HTTPS): 8
- Stage 3 (mTLS): 12
- Mapped to phases: 28
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-18*  
*Last updated: 2026-03-18 after 3-stage planning*
