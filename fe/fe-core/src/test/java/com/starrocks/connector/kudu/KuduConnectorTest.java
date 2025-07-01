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

package com.starrocks.connector.kudu;

import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class KuduConnectorTest {

    @Test
    public void testCreateKuduConnector() {
        Map<String, String> properties = new HashMap<>();

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties)),
                "The property kudu.master must be set.");

        properties.put("kudu.master", "localhost:7051");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties)),
                "The property kudu.catalog.type must be set.");

        properties.put("kudu.catalog.type", "kudu");

        new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties));
    }

    @Test
    public void testCreateHiveKuduConnectorWithoutUris() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master", "localhost:7051");
        properties.put("kudu.catalog.type", "hive");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties)),
                "The property hive.metastore.uris must be set if kudu catalog is hive.");

        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties));
    }

    @Test
    public void testCreateGlueKuduConnectorWithoutUris() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master", "localhost:7051");
        properties.put("kudu.catalog.type", "glue");
        KuduConnector connector = new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties));

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof KuduMetadata);
    }

    @Test
    public void testGetMetadata() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master", "localhost:7051");
        properties.put("kudu.catalog.type", "kudu");
        KuduConnector connector = new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties));

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof KuduMetadata);
    }

    @Test
    public void testGetMetadataWithHiveCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master", "localhost:7051");
        properties.put("kudu.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        KuduConnector connector = new KuduConnector(new ConnectorContext("kudu_catalog", "kudu", properties));

        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof KuduMetadata);
    }
}
