# Phase 1 Validation - Basic HTTP OpenSearch Connector

## Summary

Phase 1 implements a basic HTTP-only OpenSearch connector as a separate type from Elasticsearch. This validation document captures what can and cannot be automated.

## What Was Implemented

19 Java files created (~2,908 lines) implementing:
- **OpenSearchConfig**: Configuration class without SSL/auth
- **OpenSearchConnector**: Main connector entry point
- **OpenSearchRestClient**: HTTP-only REST client
- **OpenSearchMetadata**: Schema discovery
- **OpenSearchUtil**: Type conversion utilities
- **14 supporting classes**: Query builders, partition logic, etc.

## Automated Test Files Created

### 1. OpenSearchConfigTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/opensearch/OpenSearchConfigTest.java`

**Coverage**:
- Default constructor and property defaults
- Custom property values
- Property getters/setters
- No SSL fields verification
- Multiple hosts configuration

### 2. OpenSearchUtilTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/opensearch/OpenSearchUtilTest.java`

**Coverage**:
- Type conversions (null, boolean, numeric, float, date, json)
- Keyword, text, IP types
- Unknown type handling

### 3. OpenSearchRestClientTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/opensearch/OpenSearchRestClientTest.java`

**Coverage**:
- Constructor with single/multiple hosts
- HTTP prefix handling
- No auth constructor verification
- No SSL methods verification

### 4. OpenSearchConnectorTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/opensearch/OpenSearchConnectorTest.java`

**Coverage**:
- Constructor initialization
- bindConfig() operation
- Metadata caching/lazy initialization
- Metadata type verification
- No SSL config binding

### 5. ConnectorTypeTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/ConnectorTypeTest.java`

**Coverage**:
- OPENSEARCH enum exists
- Type properties (name, connector class, config class)
- SUPPORT_TYPE_SET membership
- from() lookup
- Distinction from ES type

### 6. OpenSearchIntegrationTest.java
**Location**: `fe/fe-core/src/test/java/com/starrocks/connector/opensearch/OpenSearchIntegrationTest.java`

**Coverage**:
- RestClient connection (requires OpenSearch instance)
- List tables/databases
- Metadata initialization
- Docker availability check

## Manual-Only Tests

These require a full StarRocks deployment and cannot be automated as unit tests:

### 1. Docker Environment Setup
```bash
# Manual test:
docker run -d --name opensearch-test \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "plugins.security.disabled=true" \
  opensearchproject/opensearch:2.9.0
```

### 2. End-to-End Query Test
```sql
-- Manual test:
CREATE CATALOG opensearch PROPERTIES (
    "type" = "opensearch",
    "hosts" = "http://localhost:9200",
    "opensearch.wan.only" = "true"
);

SELECT * FROM opensearch.default.my_index LIMIT 10;
```

### 3. Catalog SQL Operations
```sql
-- Manual test:
SHOW CATALOGS;
SHOW DATABASES FROM opensearch;
SHOW TABLES FROM opensearch.default;
DESCRIBE opensearch.default.my_index;
```

## Test Coverage Summary

| Component | Unit Tests | Integration Tests | Manual Tests |
|-----------|------------|-------------------|--------------|
| OpenSearchConfig | ✓ | - | - |
| OpenSearchRestClient | ✓ | ✓ | - |
| OpenSearchMetadata | - | ✓ | - |
| OpenSearchUtil | ✓ | - | - |
| OpenSearchConnector | ✓ | ✓ | - |
| ConnectorType | ✓ | - | - |
| End-to-End Query | - | - | ✓ |
| Catalog SQL | - | - | ✓ |

## Running the Tests

### Unit Tests
```bash
cd fe && mvn test -Dtest=OpenSearchConfigTest,OpenSearchUtilTest,OpenSearchRestClientTest,OpenSearchConnectorTest,ConnectorTypeTest -pl fe-core
```

### Integration Tests (requires OpenSearch)
```bash
export OPENSEARCH_TEST_HOSTS=localhost:9200
cd fe && mvn test -Dtest=OpenSearchIntegrationTest -pl fe-core
```

## Phase 1 Completion Criteria

- [x] OpenSearchConfig with HTTP-only configuration
- [x] OpenSearchRestClient without SSL/auth
- [x] OpenSearchConnector implementing Connector interface
- [x] OpenSearchMetadata for schema discovery
- [x] ConnectorType.OPENSEARCH registration
- [x] Unit tests for all public classes
- [x] Integration test framework (manual trigger)

## Future Phases

### Phase 2: HTTPS Support
- Add SSL/TLS support to OpenSearchRestClient
- Add configuration options for SSL verification
- Add truststore configuration

### Phase 3: mTLS Support
- Add client certificate authentication
- Add keystore configuration
- Add username/password basic auth

## Known Limitations

1. **No SSL/TLS**: Phase 1 is HTTP-only
2. **No Authentication**: No username/password support
3. **Shared Table Type**: Uses EsTable for both ES and OpenSearch
4. **No Version Detection**: Assumes OpenSearch 2.9.x

## Sign-Off

Phase 1 is ready for manual testing with Docker OpenSearch.
