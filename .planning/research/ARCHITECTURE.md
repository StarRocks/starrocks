# Research: Architecture for OpenSearch mTLS Connector

## Integration with Existing Elasticsearch Connector

The existing Elasticsearch connector provides the foundation:

```
fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/
├── ElasticsearchConnector.java    # Entry point
├── EsConfig.java                  # Configuration
├── EsRestClient.java              # HTTP client (NEEDS mTLS)
├── ElasticsearchMetadata.java     # Schema discovery
└── ...
```

## Recommended Approach: Extend Existing Connector

Rather than creating a new connector, **extend the existing Elasticsearch connector** to support proper mTLS. This leverages:
- Existing schema discovery logic
- Existing query translation
- Existing connector framework integration

## Component Changes

### 1. Configuration Extension (`EsConfig.java`)

**New properties to add:**
```java
@Config(key = "ssl_client_cert_path", desc = "Path to client certificate (PEM)")
private String sslClientCertPath;

@Config(key = "ssl_client_key_path", desc = "Path to client private key (PEM)")
private String sslClientKeyPath;

@Config(key = "ssl_ca_cert_path", desc = "Path to CA certificate (PEM)")
private String sslCaCertPath;

@Config(key = "ssl_client_key_password", desc = "Password for encrypted private key")
private String sslClientKeyPassword;

@Config(key = "ssl_verify_hostname", desc = "Enable hostname verification", defaultValue = "true")
private boolean sslVerifyHostname;
```

### 2. SSL Context Factory (New Class)

Create `EsSSLContextFactory`:

```java
public class EsSSLContextFactory {
    public SSLContext createSSLContext(EsConfig config) {
        // 1. Load client certificate and private key
        // 2. Load CA certificate for trust
        // 3. Create KeyManager with client credentials
        // 4. Create TrustManager with CA
        // 5. Build SSLContext
    }
}
```

### 3. Enhanced REST Client (`EsRestClient.java`)

**Current SSL implementation (trust-all):**
```java
private static SSLSocketFactory createSSLSocketFactory() {
    SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(null, new TrustManager[] {new TrustAllCerts()}, new SecureRandom());
    return sc.getSocketFactory();
}
```

**New mTLS implementation:**
```java
private SSLSocketFactory createMTLSSocketFactory() {
    SSLContext sc = SSLContext.getInstance("TLSv1.2");
    
    // Initialize with KeyManager (client cert) and TrustManager (CA)
    sc.init(
        new KeyManager[] {createKeyManager()},    // Client authentication
        new TrustManager[] {createTrustManager()}, // Server verification
        new SecureRandom()
    );
    
    return sc.getSocketFactory();
}
```

### 4. Error Handling Enhancement

Add specific error types for SSL/mTLS failures:
- `SSLCertificateException` - Invalid/expired certificates
- `SSLHandshakeException` - mTLS negotiation failure
- `HostnameVerificationException` - Hostname mismatch

## Data Flow

### Connection Establishment
```
1. Create Catalog with mTLS properties
        ↓
2. EsConfig validates certificate paths exist
        ↓
3. EsRestClient initializes with mTLS
   - Load certificates from files
   - Create SSLContext with KeyManager + TrustManager
   - Configure OkHttpClient with SSL socket factory
        ↓
4. Test connection to OpenSearch
   - SSL handshake with mutual auth
   - Verify server certificate against CA
   - Verify hostname if enabled
        ↓
5. Ready for queries
```

### Query Execution
```
SQL Query
    ↓
ElasticsearchMetadata.getTable() - Schema
    ↓
QueryConverter - Translate to ES DSL
    ↓
EsRestClient.execute() - HTTPS with mTLS
    ↓
OpenSearch Cluster
    ↓
Parse JSON response
    ↓
Return rows
```

## Security Architecture

### Certificate Storage

**Option 1: File System (Recommended for v1)**
- Certificates stored on FE nodes
- Config: file paths in catalog properties
- Pros: Simple, standard approach
- Cons: File permission management needed

**Option 2: Credential Provider (Future)**
- Integration with StarRocks credential framework
- Support for external keystores (AWS KMS, etc.)
- Pros: Better security, centralized
- Cons: More complex

### SSL/TLS Configuration

```
┌─────────────────────────────────────────────────────────┐
│                   StarRocks FE                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │           EsRestClient (OkHttp3)                │   │
│  │  ┌─────────────┐        ┌─────────────────┐    │   │
│  │  │ KeyManager  │        │  TrustManager   │    │   │
│  │  │ (Client     │        │  (CA cert for   │    │   │
│  │  │  cert+key)  │        │   server verify)│    │   │
│  │  └──────┬──────┘        └────────┬────────┘    │   │
│  │         └────────────────┬───────┘              │   │
│  │                    SSLContext                   │   │
│  │                         │                       │   │
│  └─────────────────────────┼───────────────────────┘   │
│                            │ HTTPS + Client Auth       │
└────────────────────────────┼───────────────────────────┘
                             │
┌────────────────────────────┼───────────────────────────┐
│                   OpenSearch Cluster                   │
│  ┌─────────────────────────┴───────────────────────┐   │
│  │              HTTPS Endpoint                     │   │
│  │         (Requires client certificate)           │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Build Order

1. **Phase 1**: SSL context factory with mTLS support
2. **Phase 2**: Configuration property extension
3. **Phase 3**: Integration with EsRestClient
4. **Phase 4**: Error handling and validation
5. **Phase 5**: Testing with mTLS-enabled OpenSearch

## Testing Architecture

```
┌─────────────────────────────────────────────────────────┐
│              TestContainers/OpenSearch                  │
│  ┌─────────────────────────────────────────────────┐   │
│  │         OpenSearch with Security Plugin         │   │
│  │  - HTTPS enabled                                │   │
│  │  - Client certificate authentication required   │   │
│  │  - Custom CA                                    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                          ▲
                          │ HTTPS + mTLS
                          │
┌─────────────────────────────────────────────────────────┐
│                   StarRocks FE (Test)                   │
│  ┌─────────────────────────────────────────────────┐   │
│  │         EsRestClient with mTLS Config           │   │
│  │  - Test client certificate                      │   │
│  │  - Test CA certificate                          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---
*Research for milestone v1.0: OpenSearch mTLS Connector*
