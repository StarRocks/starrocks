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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowCreateTableStmtTest {
    private ConnectContext ctx;

    @Before
    public void setUp() {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");
        ShowCreateTableStmt stmt =
                new ShowCreateTableStmt(new TableName("testDb", "testTbl"), ShowCreateTableStmt.CreateTableType.TABLE);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTable());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Table", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Create Table", stmt.getMetaData().getColumn(1).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoTbl() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.TABLE);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No Exception throws.");
    }
}