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

package com.starrocks.connector.iceberg.hive;

import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergHiveCatalogRegisterTableTest {
    @Mocked
    private HiveCatalog hiveCatalog;

    @Injectable
    private Table mockTable;

    private IcebergHiveCatalog icebergHiveCatalog;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        icebergHiveCatalog = new IcebergHiveCatalog("test_catalog", new Configuration(), properties);
        connectContext = new ConnectContext();
    }

    @Test
    public void testRegisterTableSuccess() {
        // Test successful table registration with Hive metastore
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hiveCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergHiveCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRegisterTableFailureWithHiveException() {
        // Test table registration failure when Hive catalog throws exception
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hiveCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = new RuntimeException("Hive metastore connection failed");
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> icebergHiveCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableReturnsFalseWhenTableIsNull() {
        // Test when Hive catalog returns null table
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hiveCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = null;
            }
        };

        boolean result = icebergHiveCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertFalse(result);
    }

    @Test
    public void testRegisterTableWithHDFSLocation() {
        // Test table registration with HDFS metadata file location
        String dbName = "warehouse_db";
        String tableName = "sales_data";
        String metadataFileLocation = "hdfs://cluster/warehouse/sales_data/metadata/00001-abc123.metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                hiveCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergHiveCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }
}
