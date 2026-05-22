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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenSearchConnectorTest {

    @Test
    public void testConstructor() {
        Map<String, String> properties = new HashMap<>();
        ConnectorContext context = new ConnectorContext("opensearch_test", "opensearch", properties);

        OpenSearchConnector connector = new OpenSearchConnector(context);
        
        assertNotNull(connector);
    }

    @Test
    public void testBindConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("nodes", "localhost:9200");
        ConnectorContext context = new ConnectorContext("opensearch_test", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);

        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(new String[] {"localhost:9200"});
        config.setEnableWanOnly(true);
        config.setEnableDocValueScan(true);
        config.setEnableKeywordSniff(true);
        
        // Should not throw
        connector.bindConfig(config);
        
        // After binding, metadata should be available
        ConnectorMetadata metadata = connector.getMetadata();
        assertNotNull(metadata);
        assertTrue(metadata instanceof OpenSearchMetadata);
    }

    @Test
    public void testGetMetadataReturnsSameInstance() {
        Map<String, String> properties = new HashMap<>();
        properties.put("nodes", "localhost:9200");
        ConnectorContext context = new ConnectorContext("opensearch_test", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);

        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(new String[] {"localhost:9200"});
        connector.bindConfig(config);
        
        // Metadata should be cached/lazy-initialized
        ConnectorMetadata metadata1 = connector.getMetadata();
        ConnectorMetadata metadata2 = connector.getMetadata();
        
        assertSame(metadata1, metadata2, "Metadata should be the same instance");
    }

    @Test
    public void testMetadataType() {
        Map<String, String> properties = new HashMap<>();
        properties.put("nodes", "localhost:9200");
        ConnectorContext context = new ConnectorContext("opensearch_test", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);

        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(new String[] {"localhost:9200"});
        connector.bindConfig(config);
        
        ConnectorMetadata metadata = connector.getMetadata();
        assertTrue(metadata instanceof OpenSearchMetadata);
    }

    @Test
    public void testNoSslConfigBinding() {
        // Verify that bindConfig doesn't handle SSL-related config
        Map<String, String> properties = new HashMap<>();
        ConnectorContext context = new ConnectorContext("test", "opensearch", properties);
        OpenSearchConnector connector = new OpenSearchConnector(context);
        
        OpenSearchConfig config = new OpenSearchConfig();
        config.setNodes(new String[] {"localhost:9200"});
        
        // Should not throw - SSL config not present in Phase 1
        assertDoesNotThrow(() -> connector.bindConfig(config));
    }
}
