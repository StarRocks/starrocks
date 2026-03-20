# Project State

## Current Position

Phase: 1 (HTTP Connector)  
Plan: 01-PLAN.md  
Status: Diagnosed - issues found, needs fix  
Last activity: 2026-03-20 — Phase 1 verification issues identified

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-18)

**Core value:** Users can query OpenSearch 2.9.x indices through StarRocks SQL.
**Current focus:** Phase 1 - HTTP Basic Connector (code complete)

## Current Phase

**Phase 1: HTTP Connector**

Goal: Create HTTP-only OpenSearch 2.9.x connector (no TLS, no auth)

Requirements:
- CONN-01~05: HTTP connectivity, no auth, OpenSearch 2.9.x
- SCHEMA-01~04: Schema discovery (indices, aliases, mappings)
- QUERY-01~04: Basic SELECT queries

Success Criteria:
1. ✅ Can create OpenSearch catalog with HTTP host
2. ✅ No authentication required
3. ✅ Can list OpenSearch indices as tables
4. ✅ Can execute basic SELECT queries
5. ✅ Works with OpenSearch 2.9.x

**Implementation Complete:**
- 19 Java files created in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`
- Connector registered in `ConnectorType.java`
- Ready for compilation and testing

## Stages Overview

| Stage | Protocol | Auth | Description | Status |
|-------|----------|------|-------------|--------|
| Stage 1 | HTTP | None | Basic connector, OpenSearch 2.9.x | ✅ Code complete |
| Stage 2 | HTTPS | Server cert only | One-way TLS (CA validation) | Planned |
| Stage 3 | HTTPS | Server + Client cert | Mutual TLS (mTLS) | Planned |

## Accumulated Context

### Codebase
- Location: StarRocks repository (FE in `fe/`, BE in `be/`)
- Existing connector framework at `fe/fe-core/src/main/java/com/starrocks/connector/`
- **New**: OpenSearch connector at `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`
- Key files:
  - `OpenSearchConfig.java` - Configuration class
  - `OpenSearchRestClient.java` - HTTP client
  - `OpenSearchConnector.java` - Connector entry point
  - `OpenSearchMetadata.java` - Schema discovery

### Implementation Notes
- HTTP only (no HTTPS)
- No TLS
- No auth
- Targets OpenSearch 2.9.x specifically
- Full feature parity with ES connector (doc values, keyword sniff, WAN mode)

---
*Last updated: 2026-03-18*
