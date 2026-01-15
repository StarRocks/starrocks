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

package com.starrocks.connector.iceberg.glue;

import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergGlueCatalogRegisterTableTest {
    @Mocked
    private GlueCatalog glueCatalog;

    @Injectable
    private Table mockTable;

    private IcebergGlueCatalog icebergGlueCatalog;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.region", "us-east-1");
        icebergGlueCatalog = new IcebergGlueCatalog("test_catalog", new Configuration(), properties);
        connectContext = new ConnectContext();
    }

    @Test
    public void testRegisterTableSuccess() {
        // Test successful table registration
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                glueCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergGlueCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRegisterTableFailure() {
        // Test table registration failure when underlying catalog throws exception
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                glueCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = new RuntimeException("Registration failed");
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> icebergGlueCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableReturnsFalseWhenTableIsNull() {
        // Test when registerTable returns null table
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                glueCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = null;
            }
        };

        boolean result = icebergGlueCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertFalse(result);
    }
}
