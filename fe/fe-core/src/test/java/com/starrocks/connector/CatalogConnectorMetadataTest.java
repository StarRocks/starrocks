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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.connector.metadata.TableMetaMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CatalogConnectorMetadataTest {

    private final InformationSchemaMetadata informationSchemaMetadata = new InformationSchemaMetadata("test_catalog");
    private final TableMetaMetadata metaMetadata = new TableMetaMetadata("test_catalog", "test");

    @Test
    void testListDbNames(@Mocked ConnectorMetadata connectorMetadata) {
        new Expectations() {
            {
                connectorMetadata.listDbNames((ConnectContext) any);
                result = ImmutableList.of("test_db1", "test_db2");
                times = 1;
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        List<String> dbNames = catalogConnectorMetadata.listDbNames(new ConnectContext());
        List<String> expected = ImmutableList.of("test_db1", "test_db2", InfoSchemaDb.DATABASE_NAME);
        assertEquals(expected, dbNames);
    }

    @Test
    void testListTableNames(@Mocked ConnectorMetadata connectorMetadata) {
        new Expectations() {
            {
                connectorMetadata.listTableNames((ConnectContext) any, "test_db");
                result = ImmutableList.of("test_tbl1", "test_tbl2");
                times = 1;
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        List<String> tblNames = catalogConnectorMetadata.listTableNames(new ConnectContext(), InfoSchemaDb.DATABASE_NAME);
        List<String> expected = ImmutableList.of("tables", "table_privileges", "referential_constraints",
                "key_column_usage", "routines", "schemata", "columns", "character_sets", "collations",
                "table_constraints", "engines", "user_privileges", "schema_privileges", "statistics",
                "triggers", "events", "views", "partitions", "column_privileges"
        );
        assertEquals(expected, tblNames);

        tblNames = catalogConnectorMetadata.listTableNames(new ConnectContext(), "test_db");
        expected = ImmutableList.of("test_tbl1", "test_tbl2");
        assertEquals(expected, tblNames);
    }

    @Test
    void testGetDb(@Mocked ConnectorMetadata connectorMetadata) {
        new Expectations() {
            {
                connectorMetadata.getDb((ConnectContext) any, "test_db");
                result = null;
                times = 1;
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        Database db = catalogConnectorMetadata.getDb(new ConnectContext(), "test_db");
        assertNull(db);
        assertNotNull(catalogConnectorMetadata.getDb(new ConnectContext(), InfoSchemaDb.DATABASE_NAME));
    }

    @Test
    void testDbExists(@Mocked ConnectorMetadata connectorMetadata) {
        new Expectations() {
            {
                connectorMetadata.dbExists((ConnectContext) any, "test_db");
                result = true;
                times = 1;
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        assertTrue(catalogConnectorMetadata.dbExists(new ConnectContext(), "test_db"));
        assertTrue(catalogConnectorMetadata.dbExists(new ConnectContext(), InfoSchemaDb.DATABASE_NAME));
    }

    @Test
    void testTableExists() {
        MockedJDBCMetadata mockedJDBCMetadata = new MockedJDBCMetadata(new HashMap<>());
        assertTrue(mockedJDBCMetadata.tableExists(new ConnectContext(), "db1", "tbl1"));
    }

    @Test
    void testGetTable(@Mocked ConnectorMetadata connectorMetadata) {
        new Expectations() {
            {
                connectorMetadata.getTable((ConnectContext) any, "test_db", "test_tbl");
                result = null;
                times = 1;
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        Table table = catalogConnectorMetadata.getTable(new ConnectContext(), "test_db", "test_tbl");
        assertNull(table);
        assertNotNull(catalogConnectorMetadata.getTable(new ConnectContext(), InfoSchemaDb.DATABASE_NAME, "tables"));
    }

    @Test
    void testMetadataRouting(@Mocked ConnectorMetadata connectorMetadata) throws StarRocksException {
        ConnectContext ctx = com.starrocks.common.util.Util.getOrCreateInnerContext();
        ctx.setThreadLocalInfo();
        GetRemoteFilesParams getRemoteFilesParams =
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TableVersionRange.empty()).build();

        new Expectations() {
            {
                // the following methods are always routed to normal metadata
                // therefore, we test if the normal metadata is called exactly once per method
                times = 1;

                connectorMetadata.clear();
                connectorMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
                connectorMetadata.dropTable(null);
                connectorMetadata.refreshTable("test_db", null, null, false);
                connectorMetadata.alterMaterializedView(null);
                connectorMetadata.addPartitions(ctx, null, null, null);
                connectorMetadata.dropPartition(null, null, null);
                connectorMetadata.renamePartition(null, null, null);
                connectorMetadata.createMaterializedView((CreateMaterializedViewStatement) null);
                connectorMetadata.createMaterializedView((CreateMaterializedViewStmt) null);
                connectorMetadata.dropMaterializedView(null);
                connectorMetadata.alterMaterializedView(null);
                connectorMetadata.refreshMaterializedView(null);
                connectorMetadata.cancelRefreshMaterializedView(null);
                connectorMetadata.createView(null);
                connectorMetadata.alterView(null);
                connectorMetadata.truncateTable(null, null);
                connectorMetadata.alterTableComment(null, null, null);
                connectorMetadata.finishSink("test_db", "test_tbl", null, null);
                connectorMetadata.abortSink("test_db", "test_tbl", null);
                connectorMetadata.createTableLike(null);
                connectorMetadata.createTable(null);
                connectorMetadata.createDb("test_db");
                connectorMetadata.dropDb((ConnectContext) any, "test_db", false);
                connectorMetadata.getRemoteFiles(null, getRemoteFilesParams);
                connectorMetadata.getPartitions(null, null);
                connectorMetadata.getTableStatistics(null, null, null, null, null, -1, TableVersionRange.empty());
            }
        };

        CatalogConnectorMetadata catalogConnectorMetadata = new CatalogConnectorMetadata(
                connectorMetadata,
                informationSchemaMetadata,
                metaMetadata
        );

        catalogConnectorMetadata.clear();
        catalogConnectorMetadata.listPartitionNames("test_db", "test_tbl", ConnectorMetadatRequestContext.DEFAULT);
        catalogConnectorMetadata.dropTable(null);
        catalogConnectorMetadata.refreshTable("test_db", null, null, false);
        catalogConnectorMetadata.alterMaterializedView(null);
        catalogConnectorMetadata.addPartitions(com.starrocks.common.util.Util.getOrCreateInnerContext(), null, null, null);
        catalogConnectorMetadata.dropPartition(null, null, null);
        catalogConnectorMetadata.renamePartition(null, null, null);
        catalogConnectorMetadata.createMaterializedView((CreateMaterializedViewStatement) null);
        catalogConnectorMetadata.createMaterializedView((CreateMaterializedViewStmt) null);
        catalogConnectorMetadata.dropMaterializedView(null);
        catalogConnectorMetadata.alterMaterializedView(null);
        catalogConnectorMetadata.refreshMaterializedView(null);
        catalogConnectorMetadata.cancelRefreshMaterializedView(null);
        catalogConnectorMetadata.createView(null);
        catalogConnectorMetadata.alterView(null);
        catalogConnectorMetadata.truncateTable(null, null);
        catalogConnectorMetadata.alterTableComment(null, null, null);
        catalogConnectorMetadata.finishSink("test_db", "test_tbl", null, null);
        catalogConnectorMetadata.abortSink("test_db", "test_tbl", null);
        catalogConnectorMetadata.createTableLike(null);
        catalogConnectorMetadata.createTable(null);
        catalogConnectorMetadata.createDb("test_db");
        catalogConnectorMetadata.dropDb(new ConnectContext(), "test_db", false);
        connectorMetadata.getRemoteFiles(null, getRemoteFilesParams);
        catalogConnectorMetadata.getPartitions(null, null);
        catalogConnectorMetadata.getTableStatistics(null, null, null, null, null, -1, TableVersionRange.empty());
    }
}
