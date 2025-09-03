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

package com.starrocks.connector.iceberg.rest;

import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class IcebergRESTCatalogRegisterTableTest {
    @Mocked
    private RESTSessionCatalog restCatalog;

    @Injectable
    private Table mockTable;

    private IcebergRESTCatalog icebergRESTCatalog;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        icebergRESTCatalog = new IcebergRESTCatalog(restCatalog, new Configuration());
        connectContext = new ConnectContext();
    }

    @Test
    public void testRegisterTableSuccess() {
        // Test successful table registration via REST catalog
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                restCatalog.registerTable((SessionCatalog.SessionContext) any, expectedTableId, metadataFileLocation);
                result = mockTable;
            }
        };

        boolean result = icebergRESTCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertTrue(result);
    }

    @Test
    public void testRegisterTableFailureWithRESTException() {
        // Test table registration failure when REST catalog throws exception
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                restCatalog.registerTable((SessionCatalog.SessionContext) any, expectedTableId, metadataFileLocation);
                result = new RESTException("REST API error");
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> icebergRESTCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableReturnsFalseWhenTableIsNull() {
        // Test when REST catalog returns null table
        String dbName = "test_db";
        String tableName = "test_table";
        String metadataFileLocation = "/path/to/table/metadata.json";
        TableIdentifier expectedTableId = TableIdentifier.of(dbName, tableName);

        new Expectations() {
            {
                restCatalog.registerTable((SessionCatalog.SessionContext) any, expectedTableId, metadataFileLocation);
                result = null;
            }
        };

        boolean result = icebergRESTCatalog.registerTable(connectContext, dbName, tableName, metadataFileLocation);
        Assertions.assertFalse(result);
    }
}
