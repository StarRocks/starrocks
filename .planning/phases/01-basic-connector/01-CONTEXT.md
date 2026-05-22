# Phase 1: HTTP Connector - Context

**Gathered:** 2026-03-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Create HTTP-only OpenSearch 2.9.x connector (no TLS, no authentication).

**In scope:**
- New "opensearch" connector type
- HTTP protocol only (no HTTPS)
- No authentication required
- OpenSearch 2.9.x specific
- Schema discovery (indices, aliases, mappings)
- SELECT queries with column projection
- Full feature parity with ES connector (doc values, keyword sniff, WAN mode)

**Out of scope:**
- HTTPS/TLS (Stage 2)
- mTLS/Client certificates (Stage 3)
- Username/password auth
- Elasticsearch compatibility
- Write operations (INSERT/UPDATE/DELETE)

</domain>

<decisions>
## Implementation Decisions

### Connector Type Strategy
- **New "opensearch" type** — Clean separation from ES connector
- Register in `ConnectorType.java` as: `OPENSEARCH("opensearch", OpenSearchConnector.class, OpenSearchConfig.class)`
- Support type set inclusion: `EnumSet.of(..., OPENSEARCH, ...)`

### Code Location
- **New package**: `com.starrocks.connector.opensearch`
- Location: `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`
- Clean separation following StarRocks pattern (hive, iceberg, etc.)

### Implementation Approach
- **Copy and modify from ES connector**
- Key classes to create:
  - `OpenSearchConfig` (based on `EsConfig`)
  - `OpenSearchConnector` (based on `ElasticsearchConnector`)
  - `OpenSearchRestClient` (based on `EsRestClient`)
  - `OpenSearchMetadata` (based on `ElasticsearchMetadata`)
- Remove SSL and auth requirements
- Keep all other features (doc values, keyword sniff, WAN mode)

### Feature Set
- **Full feature parity** with ES connector
- Include: doc value scan, keyword sniff, WAN only mode
- HTTP only, no SSL configuration
- No authentication (username/password optional, not required)

### OpenSearch Version
- **OpenSearch 2.9.x only**
- Version detection not required for Phase 1
- Can hardcode or simplify version-specific logic

</decisions>

<specifics>
## Specific Ideas

### Configuration Example
```sql
CREATE EXTERNAL CATALOG opensearch_catalog
PROPERTIES (
    "type" = "opensearch",
    "hosts" = "http://localhost:9200"
);
```

### Class Mapping
| ES Class | OpenSearch Class | Changes |
|----------|------------------|---------|
| `EsConfig` | `OpenSearchConfig` | Remove SSL config, make auth optional |
| `ElasticsearchConnector` | `OpenSearchConnector` | Use OpenSearch* classes |
| `EsRestClient` | `OpenSearchRestClient` | HTTP only, no auth headers |
| `ElasticsearchMetadata` | `OpenSearchMetadata` | Minimal changes |
| `EsUtil`, `MappingPhase`, etc. | Copy to opensearch package | Keep as-is |

</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing ES Connector (Reference Implementation)
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java` — Connector registration enum
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsConfig.java` — Config class to copy
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ElasticsearchConnector.java` — Connector class to copy
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsRestClient.java` — HTTP client to copy
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ElasticsearchMetadata.java` — Metadata to copy
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsUtil.java` — Utilities to copy
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/MappingPhase.java` — Mapping logic to copy

### Connector Framework
- `fe/fe-core/src/main/java/com/starrocks/connector/Connector.java` — Base interface
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorConfig.java` — Config base class
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorFactory.java` — Factory that creates connectors

### Requirements
- `.planning/REQUIREMENTS.md` — CONN-01~05, SCHEMA-01~04, QUERY-01~04

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **ES connector classes** — Copy and modify these for OpenSearch
- **OkHttp3** — Already used in EsRestClient, continue using
- **Jackson JSON** — Already used for mapping parsing

### Established Patterns
- **Connector registration** — Enum in ConnectorType, factory creates via reflection
- **Config loading** — @Config annotation for properties, loadConfig() method
- **Lazy initialization** — LazyConnector pattern for metadata

### Integration Points
- **ConnectorType enum** — Add OPENSEARCH entry
- **Catalog creation** — CREATE EXTERNAL CATALOG with type="opensearch"
- **Query planning** — Connector provides metadata for optimizer

### Key Files to Create
```
fe/fe-core/src/main/java/com/starrocks/connector/opensearch/
├── OpenSearchConfig.java
├── OpenSearchConnector.java
├── OpenSearchRestClient.java
├── OpenSearchMetadata.java
├── OpenSearchUtil.java
├── MappingPhase.java
└── ... (other copied classes)
```

</code_context>

<deferred>
## Deferred Ideas

- HTTPS support — Stage 2
- mTLS/Client certificates — Stage 3
- Username/password authentication — Future if needed
- Elasticsearch compatibility — Out of scope
- Write operations (INSERT/UPDATE/DELETE) — Future enhancement

</deferred>

---

*Phase: 01-basic-connector*
*Context gathered: 2026-03-18*
