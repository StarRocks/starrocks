# Research: Technology Stack for OpenSearch mTLS Connector

## Existing Stack Context

StarRocks already has an **Elasticsearch connector** (`fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/`) that uses:
- **HTTP Client**: OkHttp3 for REST API calls
- **Auth**: Basic authentication (username/password)
- **SSL**: Simple SSL with trust-all certificates (NOT production-ready)

## Required Stack Additions for mTLS

### 1. Client Certificate Support

| Component | Current | Required for mTLS |
|-----------|---------|-------------------|
| SSL Context | Trust-all | Mutual authentication |
| Keystore | None | Client cert + private key |
| Truststore | None | CA certificate validation |
| Hostname verification | Disabled | Enabled with proper certs |

### 2. OpenSearch Client Options

**Option A: Extend Current OkHttp3 Approach (Recommended)**
- Continue using `EsRestClient` with OkHttp3
- Add `KeyManager` for client certificates
- Add proper `TrustManager` for CA validation
- Pros: Minimal changes, consistent with existing code
- Cons: Manual SSL context management

**Option B: OpenSearch Java Client (High Level REST Client)**
- Official OpenSearch client library
- Built-in mTLS support via `SSLContext`
- Version: OpenSearch Java Client 2.x (for OpenSearch 1.x/2.x compatibility)
- Pros: Better API, official support
- Cons: Additional dependency, migration effort

**Decision**: Extend Option A - add mTLS to existing `EsRestClient`

### 3. Certificate Configuration

**Format Support** (in priority order):
1. **PEM format** (most common for OpenSearch):
   - `ssl_client_cert` - Client certificate file path
   - `ssl_client_key` - Private key file path  
   - `ssl_ca_cert` - CA certificate file path

2. **JKS/PKCS12 Keystore** (Java standard):
   - `ssl_keystore_location` - Keystore file path
   - `ssl_keystore_password` - Keystore password
   - `ssl_truststore_location` - Truststore file path
   - `ssl_truststore_password` - Truststore password

### 4. Java Security APIs

```java
// Required imports
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
```

### 5. Credential Storage Integration

StarRocks has existing credential framework:
- `com.starrocks.credential` package
- Support for cloud credentials (AWS, Azure, GCP)
- Should extend for certificate storage

**Integration points**:
- Store certificate paths (not content) in catalog properties
- Support for password encryption
- File permission validation

### 6. No New Dependencies Required

Current dependencies sufficient:
- OkHttp3 (already used)
- Java standard security libraries (javax.net.ssl)
- Apache Commons (already used)

## Configuration Schema

```properties
# Basic connection
hosts=https://opensearch-node1:9200,https://opensearch-node2:9200

# mTLS authentication (PEM format)
opensearch.ssl.client_cert_path=/etc/starrocks/certs/client.crt
opensearch.ssl.client_key_path=/etc/starrocks/certs/client.key
opensearch.ssl.ca_cert_path=/etc/starrocks/certs/ca.crt

# Optional: Key password (if encrypted)
opensearch.ssl.client_key_password=encrypted:xxx

# Alternative: JKS format
opensearch.ssl.keystore_path=/etc/starrocks/certs/keystore.jks
opensearch.ssl.keystore_password=encrypted:xxx
opensearch.ssl.truststore_path=/etc/starrocks/certs/truststore.jks
opensearch.ssl.truststore_password=encrypted:xxx

# SSL verification (default: true)
opensearch.ssl.verify_hostname=true
```

## Compatibility Matrix

| OpenSearch Version | Elasticsearch Version | Compatibility |
|-------------------|----------------------|---------------|
| 1.x | 7.10.2 | Full API compatibility |
| 2.x | N/A | Slightly different API |
| 2.x | 8.x | Some breaking changes |

**Decision**: Support OpenSearch 1.x and 2.x, Elasticsearch 7.x and 8.x

## Security Considerations

1. **Certificate file permissions**: Should be readable only by StarRocks process
2. **Password encryption**: Use existing StarRocks encryption mechanism
3. **Certificate rotation**: Support hot-reload without restart
4. **Private key protection**: Never log private key content

---
*Research for milestone v1.0: OpenSearch mTLS Connector*
