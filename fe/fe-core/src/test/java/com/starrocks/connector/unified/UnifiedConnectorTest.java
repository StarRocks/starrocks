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

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mocked;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    public void testCreateUnifiedConnectorHive() {
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", ImmutableMap.of(
                "type", "unified",
                "unified.metastore.type", "hive",
                "hive.metastore.uris", "thrift://127.0.0.1:9083"
        ));
        UnifiedConnector unifiedConnector = new UnifiedConnector(context);
        ConnectorMetadata metadata = unifiedConnector.getMetadata();
        assertTrue(metadata instanceof UnifiedMetadata);
        unifiedConnector.shutdown();
    }

    @Test
    public void testCreateUnifiedConnectorHMS() {
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", ImmutableMap.of(
                "type", "unified",
                "unified.metastore.type", "hms",
                "hive.metastore.uris", "thrift://127.0.0.1:9083"
        ));
        UnifiedConnector unifiedConnector = new UnifiedConnector(context);
        ConnectorMetadata metadata = unifiedConnector.getMetadata();
        assertTrue(metadata instanceof UnifiedMetadata);
        unifiedConnector.shutdown();
    }

    @Test
    public void testCreateUnifiedConnectorGlue(@Mocked GlueClient glueClient) {
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", ImmutableMap.of(
                "type", "unified",
                "unified.metastore.type", "glue",
                "aws.glue.access_key", "access_key",
                "aws.glue.secret_key", "secret_key"
        ));
        UnifiedConnector unifiedConnector = new UnifiedConnector(context);
        ConnectorMetadata metadata = unifiedConnector.getMetadata();
        assertTrue(metadata instanceof UnifiedMetadata);
        unifiedConnector.shutdown();
    }

    @Test
    public void testCreateUnifiedConnectorUnknown() {
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", ImmutableMap.of(
                "type", "unified",
                "unified.metastore.type", "unknown"
        ));

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class, () -> new UnifiedConnector(context));
        assertEquals(e.getMessage(), "Unified catalog only supports hms and glue as metastore.");
    }

    @Test
    public void testCreateUnifiedConnectorNull() {
        ConnectorContext context = new ConnectorContext("unified_catalog", "unified", ImmutableMap.of(
                "type", "unified"
        ));

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class, () -> new UnifiedConnector(context));
        assertEquals(e.getMessage(), "Please specify a metastore type of unified connector. " +
                "Only supports hms and glue now.");
    }
}
