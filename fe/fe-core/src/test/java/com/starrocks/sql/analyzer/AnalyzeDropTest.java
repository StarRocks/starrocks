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

import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDropTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testDropTable() {
        analyzeSuccess("drop table if exists table_to_drop force");
        analyzeSuccess("drop table if exists table_to_drop");
        analyzeSuccess("drop table table_to_drop force");
        analyzeSuccess("drop table test.table_to_drop");
        analyzeFail("drop table exists table_to_drop");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop table if exists test.table_to_drop force");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("table_to_drop", stmt.getTableName());
        Assert.assertTrue(stmt.isSetIfExists());
        Assert.assertTrue(stmt.isForceDrop());
        stmt = (DropTableStmt) analyzeSuccess("drop table t0");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("t0", stmt.getTableName());
        Assert.assertFalse(stmt.isSetIfExists());
        Assert.assertFalse(stmt.isForceDrop());
    }

    @Test
    public void testDropView() {
        analyzeSuccess("drop view if exists view_to_drop");
        analyzeSuccess("drop view view_to_drop");
        analyzeSuccess("drop view test.view_to_drop");
        analyzeFail("drop view view_to_drop force");
        analyzeFail("drop view exists view_to_drop");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop view if exists test.view_to_drop");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("view_to_drop", stmt.getTableName());
        Assert.assertTrue(stmt.isView());
        Assert.assertTrue(stmt.isSetIfExists());
        Assert.assertFalse(stmt.isForceDrop());
    }

}