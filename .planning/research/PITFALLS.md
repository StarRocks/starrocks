# Research: Pitfalls for OpenSearch mTLS Connector

## Pitfall 1: Certificate File Permissions

**Issue**: Client private key files must be readable only by StarRocks process, but this is often missed.

**Warning signs**:
- Files with world-readable permissions (644 or 666)
- Private keys readable by non-owner

**Prevention**:
```java
// Validate permissions on startup
Path keyPath = Paths.get(config.getSslClientKeyPath());
Set<PosixFilePermission> perms = Files.getPosixFilePermissions(keyPath);
if (perms.contains(PosixFilePermission.OTHERS_READ)) {
    throw new SecurityException("Private key must not be world-readable");
}
```

**Phase to address**: Phase 4 (Validation)

## Pitfall 2: Trust-All SSL Context

**Issue**: Current `EsRestClient` uses `TrustAllCerts` which accepts ANY certificate - major security vulnerability.

**Current code (BAD)**:
```java
sc.init(null, new TrustManager[] {new TrustAllCerts()}, new SecureRandom());
```

**Prevention**:
- Remove trust-all implementation
- Require CA certificate for production
- Add config option `ssl_verify_mode` (none|ca|full)

**Phase to address**: Phase 2 (SSL Context Factory)

## Pitfall 3: Certificate Expiration

**Issue**: Certificates expire, causing sudden connection failures in production.

**Warning signs**:
- Intermittent connection failures
- SSLHandshakeException with "certificate expired"

**Prevention**:
```java
// Validate certificate validity on startup
X509Certificate cert = ...;
cert.checkValidity(); // Throws if expired

// Optional: Add metric/alert for expiration
long daysUntilExpiry = ...;
if (daysUntilExpiry < 30) {
    LOG.warn("Certificate expires in {} days", daysUntilExpiry);
}
```

**Phase to address**: Phase 4 (Validation)

## Pitfall 4: Hostname Verification Bypass

**Issue**: Disabling hostname verification opens MITM attacks.

**Current code (BAD)**:
```java
private static class TrustAllHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
        return true; // NEVER do this in production!
    }
}
```

**Prevention**:
- Enable hostname verification by default
- Use OkHttp's built-in verifier: `OkHostnameVerifier`
- Only allow bypass in explicit "insecure" mode for testing

**Phase to address**: Phase 2 (SSL Context)

## Pitfall 5: Private Key Password in Plain Text

**Issue**: Private keys are often encrypted, but passwords stored in plain text.

**Prevention**:
```java
// Support encrypted passwords
@Config(key = "ssl_client_key_password")
private String sslClientKeyPassword; // Can be "encrypted:xxx"

// Decrypt using StarRocks credential util
String password = CredentialUtil.decryptIfNeeded(config.getSslClientKeyPassword());
```

**Phase to address**: Phase 2 (Configuration)

## Pitfall 6: Certificate Format Confusion

**Issue**: PEM vs JKS vs PKCS12 confusion leads to configuration errors.

**Common mistakes**:
- Trying to load PEM as JKS
- Wrong password for keystore
- Missing intermediate certificates

**Prevention**:
```java
// Auto-detect format based on file extension or content
public enum CertFormat {
    PEM,      // .pem, .crt, .key
    JKS,      // .jks
    PKCS12    // .p12, .pfx
}

// Or require explicit format specification
@Config(key = "ssl_cert_format", defaultValue = "PEM")
private String sslCertFormat;
```

**Phase to address**: Phase 1 (SSL Factory)

## Pitfall 7: Missing Intermediate Certificates

**Issue**: Certificate chain incomplete, causing SSL handshake failures.

**Warning signs**:
- "unable to find valid certification path"
- Works with some clients but not others

**Prevention**:
- Load complete certificate chain
- Support separate intermediate CA file
- Document how to create complete chain

**Phase to address**: Phase 1 (SSL Factory)

## Pitfall 8: Connection Pool with mTLS

**Issue**: SSL context per-connection vs shared context affects performance.

**Prevention**:
```java
// Create SSLContext once and reuse
private final SSLContext sslContext;

public EsRestClient(...) {
    this.sslContext = createSSLContext(config); // Do this once
}

// Use same context for all connections
OkHttpClient client = new OkHttpClient.Builder()
    .sslSocketFactory(sslContext.getSocketFactory(), trustManager)
    .build();
```

**Phase to address**: Phase 3 (Integration)

## Pitfall 9: Error Messages Leaking Sensitive Info

**Issue**: SSL error messages may contain certificate details or paths.

**Current risk**:
```java
LOG.error("SSL error with cert: {}", certPath); // May leak paths
```

**Prevention**:
```java
// Log safe information only
LOG.error("SSL handshake failed with OpenSearch cluster");
LOG.debug("Certificate path: {}", certPath); // Debug only

// Sanitize exceptions before returning to user
throw new StarRocksConnectorException("SSL authentication failed. Check certificates.");
```

**Phase to address**: Phase 4 (Error Handling)

## Pitfall 10: No Certificate Hot-Reload

**Issue**: Certificates rotated but StarRocks requires restart to pick up new ones.

**Workaround for v1**:
- Document restart requirement
- Provide metrics endpoint for expiration monitoring

**Future improvement**:
- File watcher for certificate changes
- Automatic SSL context refresh

**Phase to address**: Phase 5 (Testing/Documentation)

## Integration-Specific Pitfalls

### Pitfall 11: Breaking Existing Elasticsearch Users

**Issue**: Changes for mTLS may break existing basic-auth Elasticsearch setups.

**Prevention**:
- Keep existing `enableSsl` behavior for backward compatibility
- Add new `enableMTLS` flag
- Gradual migration path

**Phase to address**: Phase 3 (Integration)

### Pitfall 12: OpenSearch vs Elasticsearch API Differences

**Issue**: OpenSearch 2.x has some API differences from Elasticsearch.

**Prevention**:
- Test with both OpenSearch and Elasticsearch
- Handle version-specific behavior
- Clear documentation of supported versions

**Phase to address**: Phase 5 (Testing)

---
*Research for milestone v1.0: OpenSearch mTLS Connector*
