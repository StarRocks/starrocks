# StarRocks OpenSearch Connector

## What This Is

Add OpenSearch 2.9.x as an external catalog connector to StarRocks, with progressive TLS support.

Delivered in three stages:
1. **Stage 1**: HTTP-only OpenSearch connector (no TLS, no auth)
2. **Stage 2**: HTTPS with server certificate validation (one-way TLS)
3. **Stage 3**: mTLS with client certificate authentication (mutual TLS)

## Core Value

Users can query OpenSearch 2.9.x indices through StarRocks SQL with appropriate security levels.

## Current Milestone: v1.0 OpenSearch Connector

**Goal:** Build OpenSearch connector incrementally

**Stage 1 - HTTP:**
- HTTP-only OpenSearch connector
- No authentication required
- OpenSearch 2.9.x specific
- Schema discovery (indices, aliases, mappings)
- SELECT queries with column projection

**Stage 2 - HTTPS (One-way TLS):**
- HTTPS support
- Server certificate validation
- Custom CA certificate support
- Hostname verification

**Stage 3 - mTLS (Mutual TLS):**
- Client certificate authentication
- Private key configuration
- Multiple certificate formats (PEM, JKS, PKCS12)

## Requirements

### Validated

(None yet — ship to validate)

### Active

See `.planning/REQUIREMENTS.md` for detailed requirements.

**Summary (Stage 1 - HTTP):**
- HTTP-only OpenSearch connector
- No authentication
- OpenSearch 2.9.x specific
- Schema discovery
- SELECT queries

**Summary (Stage 2 - HTTPS):**
- HTTPS support
- Server certificate validation
- CA certificate configuration
- Hostname verification

**Summary (Stage 3 - mTLS):**
- Client certificate authentication
- Private key support
- Certificate format support

### Out of Scope

- Authentication in Stage 1 and 2 (no username/password)
- Elasticsearch compatibility (OpenSearch 2.9.x only)
- Write operations (INSERT/UPDATE/DELETE)
- OpenSearch DSL passthrough

## Context

- StarRocks already has Elasticsearch connector as foundation
- Existing connector framework at `fe/connector/elasticsearch/`
- Target: OpenSearch 2.9.x specifically
- Progressive TLS: HTTP → HTTPS → mTLS

## Constraints

- **Tech stack**: Java 17 for FE, extend existing connector
- **Target version**: OpenSearch 2.9.x only
- **Stage 1**: HTTP only, no TLS, no auth
- **Stage 2**: HTTPS with server cert validation only (no client cert)
- **Stage 3**: Full mTLS with client certificate

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| 3-stage approach | Progressive complexity: HTTP → HTTPS → mTLS | — Pending |
| HTTP only for Stage 1 | Simplest first step | — Pending |
| One-way TLS for Stage 2 | Server auth before mutual auth | — Pending |
| OpenSearch 2.9.x only | Focus on specific version | — Pending |

---
*Last updated: 2026-03-18 after 3-stage planning*
