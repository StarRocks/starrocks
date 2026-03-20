# Roadmap: OpenSearch Connector v1.0

## Overview

| Property | Value |
|----------|-------|
| Milestone | v1.0 |
| Phases | 3 |
| Requirements | 28 |
| Status | Planned |

## Stage 1: HTTP Basic Connector ✅

### Phase 1: HTTP Connector
**Status:** Complete  
**Goal:** Create HTTP-only OpenSearch 2.9.x connector (no TLS, no auth)

**Requirements:**
- CONN-01~05: HTTP connectivity, no auth, OpenSearch 2.9.x
- SCHEMA-01~04: Schema discovery (indices, aliases, mappings)
- QUERY-01~04: Basic SELECT queries

**Success Criteria:**
1. Can create OpenSearch catalog with HTTP host
2. No authentication required
3. Can list OpenSearch indices as tables
4. Can execute basic SELECT queries
5. Works with OpenSearch 2.9.x

---

## Stage 2: HTTPS (One-way TLS)

### Phase 2: HTTPS Support
**Goal:** Add HTTPS with server certificate validation (no client cert)

**Requirements:**
- HTTPS-01~03: HTTPS support, server certificate validation
- HOST-01~03: Hostname verification
- SEC-01~02: CA certificate validation, SSL error messages

**Success Criteria:**
1. HTTPS connections work
2. Server certificate validates against CA
3. Hostname verification works
4. Proper error messages for SSL issues

---

## Stage 3: mTLS (Mutual TLS)

### Phase 3: mTLS Support
**Goal:** Add mutual TLS with client certificate authentication

**Requirements:**
- MTLS-01~05: Client certificate and private key configuration
- CERT-01~04: Certificate format support (PEM, JKS, PKCS12)
- SECH-01~03: Security hardening, certificate expiration warnings

**Success Criteria:**
1. Client certificate authentication works
2. Private key with password support works
3. All certificate formats work
4. Certificate expiration warnings logged

---

## Traceability Summary

| Category | Count | Phases |
|----------|-------|--------|
| Basic Connectivity | 5 | Phase 1 |
| Schema Discovery | 4 | Phase 1 |
| Query Capabilities | 4 | Phase 1 |
| HTTPS Support | 3 | Phase 2 |
| Hostname Verification | 3 | Phase 2 |
| Security (Stage 2) | 2 | Phase 2 |
| mTLS Authentication | 5 | Phase 3 |
| Certificate Formats | 4 | Phase 3 |
| Security Hardening | 3 | Phase 3 |

---
*Roadmap created: 2026-03-18*  
*Next phase: Phase 1 - HTTP Connector*
