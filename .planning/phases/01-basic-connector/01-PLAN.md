# Phase 1: HTTP Connector - Plan

**Phase:** 1  
**Wave:** 1  
**Status:** Ready for execution

---

## Goal

Create HTTP-only OpenSearch 2.9.x connector (no TLS, no authentication) by copying and modifying the existing Elasticsearch connector.

---

## Task 1: Create OpenSearch Connector Package Structure

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/` (reference implementation)
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java` (registration point)
</read_first>

<action>
Create new package directory:
```bash
mkdir -p fe/fe-core/src/main/java/com/starrocks/connector/opensearch
```
</action>

<acceptance_criteria>
- Directory `fe/fe-core/src/main/java/com/starrocks/connector/opensearch` exists
- Directory is empty and ready for source files
</acceptance_criteria>

---

## Task 2: Create OpenSearchConfig

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsConfig.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/config/ConnectorConfig.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/config/Config.java` (annotation)
</read_first>

<action>
Create `OpenSearchConfig.java` in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`:

```java
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.opensearch;

import com.starrocks.connector.config.Config;
import com.starrocks.connector.config.ConnectorConfig;

public class OpenSearchConfig extends ConnectorConfig {

    public static final String KEY_HOSTS = "hosts";
    public static final String KEY_WAN_ONLY = "wan_only";
    public static final String KEY_DOC_VALUE_SCAN = "doc_value_scan";
    public static final String KEY_KEYWORD_SNIFF = "keyword_sniff";

    @Config(key = KEY_HOSTS, desc = "OpenSearch cluster hosts", defaultValue = "", nullable = false)
    private String[] nodes;

    @Config(key = KEY_WAN_ONLY,
            desc = "Only use specified hosts to access OpenSearch cluster",
            defaultValue = "true")
    private boolean enableWanOnly = true;

    @Config(key = KEY_DOC_VALUE_SCAN,
            desc = "Enable docvalues scan optimization",
            defaultValue = "true")
    private boolean enableDocValueScan = true;
    
    @Config(key = KEY_KEYWORD_SNIFF,
            desc = "Enable keyword sniffing for filtering",
            defaultValue = "true")
    private boolean enableKeywordSniff = true;

    public String[] getNodes() {
        return nodes;
    }

    public void setNodes(String[] nodes) {
        this.nodes = nodes;
    }

    public boolean isEnableWanOnly() {
        return enableWanOnly;
    }

    public void setEnableWanOnly(boolean enableWanOnly) {
        this.enableWanOnly = enableWanOnly;
    }

    public boolean isEnableDocValueScan() {
        return enableDocValueScan;
    }

    public void setEnableDocValueScan(boolean enableDocValueScan) {
        this.enableDocValueScan = enableDocValueScan;
    }

    public boolean isEnableKeywordSniff() {
        return enableKeywordSniff;
    }

    public void setEnableKeywordSniff(boolean enableKeywordSniff) {
        this.enableKeywordSniff = enableKeywordSniff;
    }
}
```

Key changes from EsConfig:
- Removed: `KEY_ES_NET_SSL`, `enableSsl`
- Removed: `KEY_USER`, `userName`
- Removed: `KEY_PASSWORD`, `password`
- Kept: hosts, wan_only, doc_value_scan, keyword_sniff
</action>

<acceptance_criteria>
- File `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchConfig.java` exists
- Class extends `ConnectorConfig`
- Contains `@Config` annotated fields for: hosts, wan_only, doc_value_scan, keyword_sniff
- No SSL-related configuration
- No username/password configuration
</acceptance_criteria>

---

## Task 3: Create OpenSearchRestClient

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsRestClient.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsNodeInfo.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardPartitions.java`
</read_first>

<action>
Create `OpenSearchRestClient.java` in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`:

Copy EsRestClient and modify:
1. Remove SSL-related code (sslEnabled, sslNetworkClient, getOrCreateSSLClient, TrustAllCerts, TrustAllHostnameVerifier, createSSLSocketFactory)
2. Simplify constructor to only take `String[] nodes` (no auth, no ssl)
3. In `execute()` method, always use `NETWORK_CLIENT` (no SSL branching)
4. Change package and class name

Key constructor signature:
```java
public OpenSearchRestClient(String[] nodes) {
    this.nodes = nodes;
    this.builder = new Request.Builder();
    this.currentNode = nodes[currentNodeIndex];
}
```

Key execute method:
```java
private String execute(String path) throws StarRocksConnectorException {
    int retrySize = nodes.length;
    StarRocksConnectorException scratchExceptionForThrow = null;
    OkHttpClient client = NETWORK_CLIENT;  // Always use plain HTTP client
    // ... rest same as EsRestClient
}
```
</action>

<acceptance_criteria>
- File `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchRestClient.java` exists
- Constructor signature: `OpenSearchRestClient(String[] nodes)`
- No SSL-related fields or methods
- No authentication headers
- Uses `OkHttpClient NETWORK_CLIENT` directly (no SSL client)
- All ES REST API methods preserved: getHttpNodes(), version(), getMapping(), searchShards(), listTables(), etc.
</acceptance_criteria>

---

## Task 4: Create OpenSearchMetadata

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ElasticsearchMetadata.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorMetadata.java` (interface)
</read_first>

<action>
Create `OpenSearchMetadata.java` in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`:

Copy ElasticsearchMetadata and modify:
1. Change package to `com.starrocks.connector.opensearch`
2. Change constructor to use `OpenSearchRestClient`
3. Keep all metadata methods (listTables, getTable, etc.)
4. Keep partition and scan logic

Key constructor:
```java
public OpenSearchMetadata(OpenSearchRestClient restClient, Map<String, String> properties, String catalogName) {
    this.restClient = restClient;
    // ... rest same
}
```
</action>

<acceptance_criteria>
- File `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchMetadata.java` exists
- Implements `ConnectorMetadata`
- Constructor takes `OpenSearchRestClient`, `Map<String, String>`, `String`
- Methods: listTableNames(), getTable(), listPartitionNames(), getPrunedPartitions(), etc.
</acceptance_criteria>

---

## Task 5: Copy Utility Classes

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsUtil.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/MappingPhase.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/PartitionPhase.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/VersionPhase.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/QueryBuilders.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/QueryConverter.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/SearchContext.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/SearchPhase.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsNodeInfo.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardPartitions.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardRouting.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsTablePartitions.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsMajorVersion.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsMetaStateTracker.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsRepository.java`
</read_first>

<action>
Copy the following files from `elasticsearch` package to `opensearch` package:

1. `EsUtil.java` → `OpenSearchUtil.java`
2. `MappingPhase.java` → `MappingPhase.java`
3. `PartitionPhase.java` → `PartitionPhase.java`
4. `VersionPhase.java` → `VersionPhase.java`
5. `QueryBuilders.java` → `QueryBuilders.java`
6. `QueryConverter.java` → `QueryConverter.java`
7. `SearchContext.java` → `SearchContext.java`
8. `SearchPhase.java` → `SearchPhase.java`
9. `EsNodeInfo.java` → `OpenSearchNodeInfo.java`
10. `EsShardPartitions.java` → `OpenSearchShardPartitions.java`
11. `EsShardRouting.java` → `OpenSearchShardRouting.java`
12. `EsTablePartitions.java` → `OpenSearchTablePartitions.java`
13. `EsMajorVersion.java` → `OpenSearchMajorVersion.java`
14. `EsMetaStateTracker.java` → `OpenSearchMetaStateTracker.java`
15. `EsRepository.java` → `OpenSearchRepository.java`

For each file:
1. Change package declaration from `com.starrocks.connector.elasticsearch` to `com.starrocks.connector.opensearch`
2. Change class name prefix from `Es` to `OpenSearch` (where applicable)
3. Update import statements to reference opensearch package classes
4. Keep all logic unchanged
</action>

<acceptance_criteria>
- All 15 utility classes exist in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`
- All classes have correct package declaration: `com.starrocks.connector.opensearch`
- Class names use `OpenSearch` prefix (not `Es`)
- Import statements updated to reference opensearch package
- All logic preserved from original ES classes
</acceptance_criteria>

---

## Task 6: Create OpenSearchConnector

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ElasticsearchConnector.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/Connector.java` (interface)
</read_first>

<action>
Create `OpenSearchConnector.java` in `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/`:

```java
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.opensearch;

import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OpenSearchConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(OpenSearchConnector.class);

    private final String catalogName;
    private OpenSearchConfig config;
    private OpenSearchRestClient restClient;
    private ConnectorMetadata metadata;

    public OpenSearchConnector(ConnectorContext context) {
        this.catalogName = context.getCatalogName();
    }

    @Override
    public ConnectorMetadata getMetadata() {
        if (metadata == null) {
            try {
                metadata = new OpenSearchMetadata(restClient, config.getProperties(), catalogName);
            } catch (StarRocksConnectorException e) {
                LOG.error("Failed to create OpenSearch metadata on [catalog : {}]", catalogName, e);
                throw e;
            }
        }
        return metadata;
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        this.config = (OpenSearchConfig) config;
        this.restClient = new OpenSearchRestClient(this.config.getNodes());
    }
}
```

Key differences from ElasticsearchConnector:
- Uses `OpenSearchConfig` instead of `EsConfig`
- Uses `OpenSearchRestClient` with only nodes parameter (no auth, no SSL)
- Uses `OpenSearchMetadata` instead of `ElasticsearchMetadata`
</action>

<acceptance_criteria>
- File `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchConnector.java` exists
- Implements `Connector`
- Constructor: `OpenSearchConnector(ConnectorContext context)`
- Method `bindConfig()`: Creates `OpenSearchRestClient` with only nodes (no auth, no SSL)
- Method `getMetadata()`: Returns `OpenSearchMetadata`
</acceptance_criteria>

---

## Task 7: Register OpenSearch Connector Type

<read_first>
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchConnector.java`
- `fe/fe-core/src/main/java/com/starrocks/connector/opensearch/OpenSearchConfig.java`
</read_first>

<action>
Add OpenSearch connector registration in `ConnectorType.java`:

1. Import the OpenSearch classes:
```java
import com.starrocks.connector.opensearch.OpenSearchConnector;
import com.starrocks.connector.opensearch.OpenSearchConfig;
```

2. Add enum entry:
```java
OPENSEARCH("opensearch", OpenSearchConnector.class, OpenSearchConfig.class),
```

3. Add to SUPPORT_TYPE_SET:
```java
public static final Set<ConnectorType> SUPPORT_TYPE_SET = EnumSet.of(
        ES,
        OPENSEARCH,  // Add this
        HIVE,
        ICEBERG,
        JDBC,
        HUDI,
        DELTALAKE,
        PAIMON,
        ODPS,
        KUDU,
        UNIFIED,
        BENCHMARK
);
```
</action>

<acceptance_criteria>
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java` has import for `OpenSearchConnector` and `OpenSearchConfig`
- Enum entry `OPENSEARCH("opensearch", OpenSearchConnector.class, OpenSearchConfig.class)` exists
- `OPENSEARCH` included in `SUPPORT_TYPE_SET`
- File compiles without errors
</acceptance_criteria>

---

## Task 8: Build and Test

<read_first>
- `fe/pom.xml` (parent POM)
- `build.sh` or build instructions in project root
</read_first>

<action>
Build the OpenSearch connector to verify it compiles:

```bash
cd fe
mvn clean compile -pl fe-core -am -DskipTests
```

Verify:
1. All OpenSearch classes compile without errors
2. No missing imports
3. No type mismatches
</action>

<acceptance_criteria>
- Build completes successfully: `BUILD SUCCESS` in Maven output
- No compilation errors in `opensearch` package
- No missing symbol errors
</acceptance_criteria>

---

## Task 9: Manual Test Script

<read_first>
- Test environment setup documentation
- OpenSearch 2.9.x Docker image info
</read_first>

<action>
Create manual test instructions:

1. Start OpenSearch 2.9.x (Docker):
```bash
docker run -d --name opensearch-29 \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  opensearchproject/opensearch:2.9.0
```

2. Create test catalog in StarRocks:
```sql
CREATE EXTERNAL CATALOG opensearch_test
PROPERTIES (
    "type" = "opensearch",
    "hosts" = "http://localhost:9200"
);
```

3. Verify connection:
```sql
SHOW CATALOGS;
USE opensearch_test;
SHOW TABLES;
```

4. Test query (after creating index with sample data):
```sql
SELECT * FROM your_index LIMIT 10;
```
</action>

<acceptance_criteria>
- Test script exists in `.planning/phases/01-basic-connector/test-manual.md`
- Docker command starts OpenSearch 2.9.x
- SQL commands create and use catalog
- Query returns data from OpenSearch index
</acceptance_criteria>

---

## Must-Haves for Phase 1 Success

1. **Connector Registration**: OPENSEARCH type registered in ConnectorType.java
2. **HTTP Only**: No SSL/TLS code paths, no authentication required
3. **Schema Discovery**: Can list OpenSearch indices as tables
4. **Basic Query**: Can execute SELECT queries on OpenSearch indices
5. **OpenSearch 2.9.x**: Compatible with target version

---

## Verification Checklist

- [ ] All 15+ source files created in opensearch package
- [ ] ConnectorType.java updated with OPENSEARCH enum
- [ ] Build compiles successfully
- [ ] Manual test passes (catalog creation, table listing, SELECT query)

---

*Phase 1 Plan*  
*Created: 2026-03-18*
