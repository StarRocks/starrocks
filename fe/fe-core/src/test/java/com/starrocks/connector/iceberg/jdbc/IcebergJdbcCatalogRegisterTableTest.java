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

package com.starrocks.connector.iceberg.jdbc;

import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergJdbcCatalogRegisterTableTest {
    @Mocked
    private JdbcCatalog jdbcCatalog;

    @Injectable
    private Table mockTable;

    private IcebergJdbcCatalog icebergJdbcCatalog;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog");
        properties.put("iceberg.catalog.jdbc.user", "iceberg");
        properties.put("iceberg.catalog.jdbc.password", "password");
        properties.put("iceberg.catalog.warehouse", "s3://my_bucket/warehouse_location");
        icebergJdbcCatalog = new IcebergJdbcCatalog("test_catalog", new Configuration(), properties);
        connectContext = new ConnectContext();
    }

    @Test
    public void testRegisterTableSuccess() {
        // Test successful table registration with JDBC catalog
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                jdbcCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergJdbcCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRegisterTableFailureWithJdbcException() {
        // Test table registration failure when JDBC catalog throws exception
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                jdbcCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = new RuntimeException("Database connection failed");
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> icebergJdbcCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableReturnsFalseWhenTableIsNull() {
        // Test when JDBC catalog returns null table
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/warehouse/test_db/test_table/metadata/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                jdbcCatalog.registerTable(expectedTableId, metadataFileLocation);
                result = null;
            }
        };

        boolean result = icebergJdbcCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertFalse(result);
    }
}
