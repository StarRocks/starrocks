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

package com.starrocks.connector;

import com.starrocks.connector.opensearch.OpenSearchConfig;
import com.starrocks.connector.opensearch.OpenSearchConnector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorTypeTest {

    @Test
    public void testOpenSearchTypeExists() {
        ConnectorType type = ConnectorType.OPENSEARCH;
        assertNotNull(type);
        assertEquals("opensearch", type.name());
    }

    @Test
    public void testOpenSearchTypeProperties() {
        ConnectorType type = ConnectorType.OPENSEARCH;
        
        // Verify catalog type
        assertEquals("opensearch", type.toString());
        
        // Verify connector class
        assertEquals(OpenSearchConnector.class, type.getConnectorFactory().getClass());
        
        // Verify config class
        assertEquals(OpenSearchConfig.class, type.getConfigClazz());
    }

    @Test
    public void testOpenSearchInSupportTypeSet() {
        // OPENSEARCH should be in the SUPPORT_TYPE_SET
        assertTrue(ConnectorType.SUPPORT_TYPE_SET.contains("opensearch"));
    }

    @Test
    public void testFromOpenSearch() {
        ConnectorType type = ConnectorType.from("opensearch");
        assertEquals(ConnectorType.OPENSEARCH, type);
    }

    @Test
    public void testOpenSearchDifferentFromEs() {
        // OpenSearch and ES should be separate types
        assertNotEquals(ConnectorType.OPENSEARCH, ConnectorType.ES);
        assertNotEquals(ConnectorType.ES.toString(), ConnectorType.OPENSEARCH.toString());
    }

    @Test
    public void testAllExpectedTypesExist() {
        // Verify all major connector types exist
        assertNotNull(ConnectorType.ES);
        assertNotNull(ConnectorType.OPENSEARCH);
        assertNotNull(ConnectorType.HIVE);
        assertNotNull(ConnectorType.ICEBERG);
        assertNotNull(ConnectorType.HUDI);
        assertNotNull(ConnectorType.JDBC);
    }
}
