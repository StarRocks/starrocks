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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorFactory;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DeltaLakeConnectorTest {
    @Test
    public void testCreateDeltaLakeConnector() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://localhost:9083");
        DeltaLakeConnector connector = new DeltaLakeConnector(new ConnectorContext("delta0", "deltalake",
                properties));
        ConnectorMetadata metadata = connector.getMetadata();
        Assertions.assertTrue(metadata instanceof DeltaLakeMetadata);
        DeltaLakeMetadata deltaLakeMetadata = (DeltaLakeMetadata) metadata;
        Assertions.assertEquals("delta0", deltaLakeMetadata.getCatalogName());
        Assertions.assertEquals(deltaLakeMetadata.getMetastoreType(), MetastoreType.HMS);
    }

    @Test
    public void testCreateDeltaLakeConnectorWithException1() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.TYPE", "glue", "aws.glue.access_key", "xxxxx",
                "aws.glue.secret_key", "xxxx",
                "aws.glue.region", "us-west-2");
        try {
            ConnectorFactory.createConnector(new ConnectorContext("delta0", "deltalake", properties), false);
            Assertions.fail("Should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertEquals("Failed to init connector [type: deltalake, name: delta0]. msg: " +
                            "hive.metastore.uris must be set in properties when creating catalog of hive-metastore",
                    e.getMessage());
        }
    }

    @Test
    public void testCreateDeltaLakeConnectorWithException2() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "error_metastore", "aws.glue.access_key", "xxxxx",
                "aws.glue.secret_key", "xxxx",
                "aws.glue.region", "us-west-2");
        try {
            ConnectorFactory.createConnector(new ConnectorContext("delta0", "deltalake", properties), false);
            Assertions.fail("Should throw exception");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertEquals("Failed to init connector [type: deltalake, name: delta0]. " +
                    "msg: Getting analyzing error. Detail message: hive metastore type [error_metastore] " +
                    "is not supported.", e.getMessage());
        }
    }

    @Test
    public void testDeltaLakeConnectorMemUsage() {
        Map<String, String> properties = ImmutableMap.of("type", "deltalake",
                "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://localhost:9083");
        CatalogConnector catalogConnector = ConnectorFactory.createConnector(
                new ConnectorContext("delta0", "deltalake", properties), false);
        Assertions.assertTrue(catalogConnector.supportMemoryTrack());
        Assertions.assertEquals(840, catalogConnector.estimateSize());
        Assertions.assertEquals(4, catalogConnector.estimateCount().size());
    }

    @Test
    public void testDeltaLakeRemoteFileInfo() {
        FileScanTask fileScanTask = null;
        DeltaRemoteFileInfo deltaRemoteFileInfo = new DeltaRemoteFileInfo(fileScanTask);
        Assertions.assertNull(deltaRemoteFileInfo.getFileScanTask());
    }
}
