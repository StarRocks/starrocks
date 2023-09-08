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

package com.starrocks.connector.unified;

import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.ConnectorMetadata;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnifiedConnectorTest {

    @Test
    public void testCreateUnifiedConnectorFromConnectorFactory() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unified");
        properties.put("unified.metastore.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", properties);
        CatalogConnector catalogConnector = ConnectorFactory.createConnector(context);
        ConnectorMetadata metadata = catalogConnector.getMetadata();
        assertTrue(metadata instanceof CatalogConnectorMetadata);
        catalogConnector.shutdown();
    }

    @Test
    public void testCreateUnifiedConnector() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "unified");
        properties.put("unified.metastore.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", properties);
        UnifiedConnector unifiedConnector = new UnifiedConnector(context);
        ConnectorMetadata metadata = unifiedConnector.getMetadata();
        assertTrue(metadata instanceof UnifiedMetadata);
        unifiedConnector.shutdown();
    }
}
