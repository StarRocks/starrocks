// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowMaterializedViewTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowMaterializedViewStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowMaterializedViewTest {
    private ConnectContext ctx;

    @Before
    public void setUp() {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("");

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("id", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("name", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("database_name", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("text", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("rows", stmt.getMetaData().getColumn(4).getName());

        stmt = new ShowMaterializedViewStmt("abc", (String) null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("abc", stmt.getDb());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("id", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("name", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("database_name", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("text", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("rows", stmt.getMetaData().getColumn(4).getName());

        stmt = new ShowMaterializedViewStmt("abc", "bcd");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("bcd", stmt.getPattern());
        Assert.assertEquals("abc", stmt.getDb());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("id", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("name", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("database_name", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("text", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("rows", stmt.getMetaData().getColumn(4).getName());

        stmt = (ShowMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(
                "SHOW MATERIALIZED VIEW FROM abc where name = 'mv1';", ctx);
        Assert.assertEquals("abc", stmt.getDb());
        Assert.assertEquals(
                "SELECT MATERIALIZED_VIEW_ID AS id, TABLE_NAME AS name, TABLE_SCHEMA AS database_name, MATERIALIZED_VIEW_DEFINITION AS text, TABLE_ROWS AS rows FROM information_schema.materialized_views WHERE TABLE_NAME = 'mv1'",
                AST2SQL.toString(stmt.toSelectStmt()));
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("id", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("name", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("database_name", stmt.getMetaData().getColumn(2).getName());
        Assert.assertEquals("text", stmt.getMetaData().getColumn(3).getName());
        Assert.assertEquals("rows", stmt.getMetaData().getColumn(4).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}
