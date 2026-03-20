# Research Summary: OpenSearch mTLS Connector

## Overview

This research covers adding mutual TLS (mTLS) authentication support to StarRocks' existing Elasticsearch connector, enabling secure connections to OpenSearch clusters that require client certificate authentication.

## Key Findings

### Stack Recommendations

| Component | Recommendation |
|-----------|----------------|
| HTTP Client | Continue with OkHttp3 (already used) |
| Certificate Format | PEM (primary), JKS/PKCS12 (secondary) |
| SSL/TLS | Java standard `javax.net.ssl` APIs |
| Dependencies | No new dependencies required |

### Architecture Decision

**Extend existing Elasticsearch connector** rather than creating new:
- Leverages existing schema discovery and query translation
- Maintains backward compatibility
- Simpler implementation
- Located in `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/`

### Critical Security Issues in Current Code

1. **Trust-all certificates** - Current implementation accepts ANY certificate
2. **Disabled hostname verification** - Vulnerable to MITM attacks
3. **No client certificate support** - Cannot do mutual TLS

These must be addressed for production mTLS use.

## Feature Priorities

### P0 (MVP Must-Have)
- mTLS with PEM certificates (client cert, private key, CA cert)
- Hostname verification
- Index listing and schema discovery
- Basic SELECT queries

### P1 (Important)
- Predicate pushdown
- JKS/PKCS12 support
- Password encryption for private keys

### P2 (Future)
- Certificate hot-reload
- Advanced query pushdown
- Certificate expiration monitoring

## Configuration Schema

```properties
# Connection
hosts=https://opensearch:9200

# mTLS (PEM format)
opensearch.ssl.client_cert_path=/path/to/client.crt
opensearch.ssl.client_key_path=/path/to/client.key
opensearch.ssl.ca_cert_path=/path/to/ca.crt

# Security
opensearch.ssl.verify_hostname=true
```

## Watch Out For

1. **Certificate file permissions** - Private keys must not be world-readable
2. **Certificate expiration** - Monitor and alert before expiry
3. **Format confusion** - Clear documentation on PEM vs JKS
4. **Error message leakage** - Don't log sensitive paths
5. **Backward compatibility** - Don't break existing Elasticsearch users

## Implementation Phases

1. **SSL Context Factory** - Create mTLS-capable SSL context
2. **Configuration Extension** - Add mTLS properties to `EsConfig`
3. **Client Integration** - Wire mTLS into `EsRestClient`
4. **Validation & Errors** - Security validation, proper error messages
5. **Testing** - mTLS-enabled OpenSearch test environment

## Files to Modify

- `EsConfig.java` - Add mTLS configuration properties
- `EsRestClient.java` - Replace trust-all SSL with mTLS
- New: `EsSSLContextFactory.java` - SSL context creation

## Files to Reference

- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/` - Existing connector
- `fe/fe-core/src/main/java/com/starrocks/credential/` - Credential framework
- `fe/fe-core/src/main/java/com/starrocks/common/Config.java` - SSL config examples

---
*Research completed: 2026-03-18*
*For milestone v1.0: OpenSearch mTLS Connector*
