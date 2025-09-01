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

package com.starrocks.connector.iceberg.hadoop;

import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergHadoopCatalogRegisterTableTest {
    @Mocked
    private HadoopCatalog hadoopCatalog;

    @Injectable
    private Table mockTable;

    private IcebergHadoopCatalog icebergHadoopCatalog;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/iceberg-warehouse");
        icebergHadoopCatalog = new IcebergHadoopCatalog("test_catalog", new Configuration(), properties);
        connectContext = new ConnectContext();
    }

    @Test
    public void testRegisterTableSuccess() {
        // Test successful table registration with Hadoop catalog
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/tmp/iceberg-warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hadoopCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergHadoopCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRegisterTableFailureWithHadoopException() {
        // Test table registration failure when Hadoop catalog throws exception
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/tmp/iceberg-warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hadoopCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = new RuntimeException("Hadoop filesystem access failed");
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> icebergHadoopCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableReturnsFalseWhenTableIsNull() {
        // Test when Hadoop catalog returns null table
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/tmp/iceberg-warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hadoopCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = null;
            }
        };

        boolean result = icebergHadoopCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertFalse(result);
    }

    @Test
    public void testRegisterTableWithHDFSWarehouseLocation() {
        // Test table registration with HDFS warehouse location
        String dbName = "production";
        String tableName = "orders";
        String metadataFileLocation = "hdfs://namenode:8020/warehouse/production/orders/metadata/v1.metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations(icebergHadoopCatalog) {
            {
                hadoopCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergHadoopCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }
}
