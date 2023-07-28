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
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeCreateTableLikeTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) analyzeSuccess(
                "CREATE TABLE tbl2 LIKE t0");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("tbl2", stmt.getTableName());
        Assert.assertEquals("test", stmt.getExistedDbName());
        Assert.assertEquals("t0", stmt.getExistedTableName());
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
    public void testNotExistCatalog() {
        analyzeFail("CREATE TABLE test.table2 LIKE ice.db1.t0", "Catalog ice is not found");
    }

    @Test
    public void testCreateTableLikeIcebergTable() {
        CatalogMgr catalogMgr = AnalyzeTestUtil.getConnectContext().getGlobalStateMgr().getCatalogMgr();
        Map<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        new Expectations(catalogMgr) {
            {
                catalogMgr.getCatalogByName("ice");
                result = new Catalog(1111, "ice", config, "");
            }
        };

        analyzeFail("CREATE TABLE test.table2 LIKE ice.db1.t0", "Table of iceberg catalog doesn't support [CREATE TABLE LIKE]");
    }
}
