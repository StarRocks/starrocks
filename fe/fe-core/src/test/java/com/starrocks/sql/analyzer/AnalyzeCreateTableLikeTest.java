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

import com.starrocks.sql.ast.CreateTableLikeStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
        analyzeSuccess("CREATE TABLE test1.table2 LIKE db1.t0;");
        analyzeSuccess("CREATE TABLE test1.table2 LIKE db2.t0;");
        analyzeSuccess("CREATE TABLE table2 LIKE t0;");
    }

    @Test
    public void testCreateTableLike() throws Exception {
        analyzeSuccess("CREATE EXTERNAL TABLE hive_test_like LIKE db1.t0");
    }
}
