# Research: OpenSearch Connector Features

## Table Stakes (Must Have)

These features are required for a production-ready OpenSearch connector with mTLS:

### 1. Core Connectivity
| Feature | Priority | Description |
|---------|----------|-------------|
| mTLS Authentication | P0 | Client certificate + CA certificate validation |
| Hostname Verification | P0 | Verify server identity against certificate |
| Connection Pooling | P1 | Reuse connections for performance |
| Retry Logic | P1 | Automatic retry on node failures |
| Timeout Configuration | P1 | Configurable connection/read timeouts |

### 2. Schema Discovery
| Feature | Priority | Description |
|---------|----------|-------------|
| Index Listing | P0 | List all non-system indices |
| Alias Support | P0 | Treat aliases as tables |
| Mapping Conversion | P0 | Convert ES mapping to StarRocks schema |
| Type Inference | P1 | Infer StarRocks types from ES types |
| Nested Field Support | P2 | Handle nested/complex types |

### 3. Query Capabilities
| Feature | Priority | Description |
|---------|----------|-------------|
| Basic SELECT | P0 | Query data from OpenSearch |
| Column Projection | P0 | Push down selected columns |
| Predicate Pushdown | P1 | Push filters to OpenSearch |
| Limit Pushdown | P1 | Push LIMIT to reduce data transfer |
| Count Optimization | P1 | Use ES count API for count(*) |

### 4. Security
| Feature | Priority | Description |
|---------|----------|-------------|
| PEM Certificate Support | P0 | Most common format |
| JKS/PKCS12 Support | P1 | Java standard format |
| Password Encryption | P1 | Secure credential storage |
| Certificate Hot-Reload | P2 | No restart required for rotation |

## Differentiators (Competitive Advantage)

Features that could set this implementation apart:

### 1. Advanced Query Pushdown
- Aggregation pushdown (when possible)
- Sort pushdown
- Script field support

### 2. Observability
- Connection metrics
- Query latency tracking
- Certificate expiration warnings

### 3. Operational Features
- Certificate expiration monitoring
- Health checks for all configured nodes
- Detailed error messages for SSL issues

## Anti-Features (Deliberately NOT Building)

| Feature | Reason |
|---------|--------|
| Write operations (INSERT/UPDATE/DELETE) | Out of scope for v1, focus on analytics |
| Index management (CREATE/DROP INDEX) | Administrative tasks, not query workload |
| OpenSearch DSL passthrough | Maintain SQL abstraction |
| Cross-cluster search | Complex, defer to v2 |
| Custom analyzers/query parsers | Too OpenSearch-specific |

## Feature Dependencies

```
mTLS Authentication
    ├── Certificate File Configuration
    ├── SSL Context Initialization
    └── Hostname Verification

Schema Discovery
    ├── Index Listing API
    ├── Mapping API
    └── Type Conversion Logic

Query Pushdown
    ├── Predicate Analysis
    ├── ES Query DSL Generation
    └── Result Parsing
```

## Complexity Assessment

| Feature Area | Complexity | Risk |
|--------------|------------|------|
| mTLS Configuration | Medium | Certificate handling bugs |
| SSL Context Setup | Medium | Security vulnerabilities |
| Schema Discovery | Low | Well-understood patterns |
| Query Pushdown | High | ES DSL complexity |
| Error Handling | Medium | SSL error messages |

## Prioritization for v1.0

**P0 (Must have for MVP)**:
1. mTLS with PEM certificates
2. Index listing and schema discovery
3. Basic SELECT with column projection
4. Hostname verification

**P1 (Important but can follow)**:
1. Predicate pushdown
2. JKS/PKCS12 support
3. Connection pooling improvements

**P2 (Nice to have)**:
1. Certificate hot-reload
2. Advanced query pushdown
3. Observability metrics

---
*Research for milestone v1.0: OpenSearch mTLS Connector*
