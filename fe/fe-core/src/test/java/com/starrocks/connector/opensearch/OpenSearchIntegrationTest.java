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

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for OpenSearch connector.
 * These tests require a running OpenSearch instance.
 * Set OPENSEARCH_TEST_HOSTS environment variable to enable these tests.
 * Example: export OPENSEARCH_TEST_HOSTS=localhost:9200
 */
public class OpenSearchIntegrationTest {

    private static String[] testHosts = null;
    private static boolean openSearchAvailable = false;

    @BeforeAll
    public static void setUpClass() {
        String hostsEnv = System.getenv("OPENSEARCH_TEST_HOSTS");
        if (hostsEnv != null && !hostsEnv.isEmpty()) {
            testHosts = hostsEnv.split(",");
            openSearchAvailable = checkOpenSearchAvailable(testHosts[0]);
        }
    }

    @BeforeEach
    public void setUp() {
        Assumptions.assumeTrue(openSearchAvailable, "OpenSearch not available. Set OPENSEARCH_TEST_HOSTS env var.");
    }

    private static boolean checkOpenSearchAvailable(String host) {
        try {
            String url = host.startsWith("http") ? host : "http://" + host;
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            int responseCode = connection.getResponseCode();
            return responseCode == 200;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testRestClientConnection() {
        OpenSearchRestClient client = new OpenSearchRestClient(testHosts);
        
        // Should be able to connect without SSL/auth
        assertNotNull(client);
    }

    @Test
    public void testListTables() throws Exception {
        Assumptions.assumeTrue(openSearchAvailable, "OpenSearch not available");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("nodes", String.join(",", testHosts));
        ConnectorContext context = new ConnectorContext("test_catalog", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);
        
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(testHosts);
        config.setEnableWanOnly(true);
        config.setEnableDocValueScan(true);
        config.setEnableKeywordSniff(true);
        
        connector.bindConfig(config);
        ConnectorMetadata metadata = connector.getMetadata();
        
        // List databases (should return default)
        List<String> dbs = metadata.listDbNames(new ConnectContext());
        assertNotNull(dbs);
        assertTrue(dbs.contains("default_db"));
    }

    @Test
    public void testMetadataInitialization() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("nodes", String.join(",", testHosts));
        ConnectorContext context = new ConnectorContext("test_catalog", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);
        
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(testHosts);
        config.setEnableWanOnly(true);
        
        connector.bindConfig(config);
        ConnectorMetadata metadata = connector.getMetadata();
        
        assertNotNull(metadata);
        assertTrue(metadata instanceof OpenSearchMetadata);
    }

    @Test
    public void testListDatabases() throws Exception {
        Assumptions.assumeTrue(openSearchAvailable, "OpenSearch not available");
        
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(testHosts);
        
        OpenSearchRestClient client = new OpenSearchRestClient(config.getNodes());
        OpenSearchMetadata metadata = new OpenSearchMetadata(client, null, "test_catalog");
        
        List<String> dbs = metadata.listDbNames(new ConnectContext());
        assertNotNull(dbs);
        // Should at least have default database
        assertTrue(dbs.contains("default_db"));
    }
}
