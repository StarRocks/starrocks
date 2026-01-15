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


package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableOperation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeCreateTableLikeTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) analyzeSuccess(
                "CREATE TABLE tbl2 LIKE t0");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("tbl2", stmt.getTableName());
        Assertions.assertEquals("test", stmt.getExistedDbName());
        Assertions.assertEquals("t0", stmt.getExistedTableName());
    }

    @Test
    public void testAnalyzeSuccess() {
        analyzeSuccess("CREATE TABLE test.table2 LIKE db1.t0;");
        analyzeSuccess("CREATE TABLE test.table2 LIKE db2.t0;");
        analyzeSuccess("CREATE TABLE table2 LIKE t0;");
    }

    @Test
    public void testCreateTableLike() throws Exception {
        analyzeSuccess("CREATE EXTERNAL TABLE hive_test_like LIKE db1.t0");

    }

    @Test
    public void testCreateIfNotExists() {
        analyzeSuccess("create table if not exists t2 like t1");
    }

    @Test
    public void testNotExistCatalog(@Mocked Table table) {
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        new Expectations(metadataMgr) {
            {
                metadataMgr.getTable((ConnectContext) any, anyString, anyString, anyString);
                result = table;
                minTimes = 0;

                table.getCatalogName();
                result = "ice";
                minTimes = 0;
            }
        };
        analyzeFail("CREATE TABLE test.table2 LIKE ice.db1.t0", "Catalog ice is not found");
    }

    @Test
    public void testCreateTableLikeIcebergTable(@Mocked IcebergTable table, @Mocked Catalog catalog) {
        MetadataMgr metadataMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getMetadataMgr();
        CatalogMgr catalogMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getCatalogMgr();
        new Expectations(metadataMgr, catalogMgr) {
            {
                metadataMgr.getTable((ConnectContext) any, anyString, anyString, anyString);
                result = table;
                minTimes = 0;

                table.getCatalogName();
                result = "ice";
                minTimes = 0;

                catalogMgr.getCatalogByName(anyString);
                result = catalog;
                minTimes = 0;

                table.getSupportedOperations();
                result = Set.of(TableOperation.ALTER);
                minTimes = 0;

                table.getType();
                result = Table.TableType.ICEBERG;
                minTimes = 0;
            }
        };

        analyzeFail("CREATE TABLE test.table2 LIKE ice.db1.t0", "Table of ICEBERG catalog doesn't support [CREATE_TABLE_LIKE].");
    }
}
