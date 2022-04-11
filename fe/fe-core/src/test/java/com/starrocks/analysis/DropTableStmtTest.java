// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DropTableStmtTest.java

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

import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropTableStmtTest {
    private TableName tbl;
    private TableName noDbTbl;
    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
        tbl = new TableName("db1", "table1");
        noDbTbl = new TableName("", "table1");
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCluster("testCluster");
    }

    @Test
    public void testNormal() throws UserException {
        DropTableStmt stmt = new DropTableStmt(false, tbl, true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testCluster:db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `testCluster:db1`.`table1`", stmt.toString());
    }

    @Test
    public void testDefaultNormal() {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl, true);
        ctx.setDatabase("testDb");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testCluster:testDb", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `testCluster:testDb`.`table1`", stmt.toSql());
    }

    @Test(expected = SemanticException.class)
    public void testNoDbFail() {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl, true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No Exception throws.");
    }

    @Test(expected = SemanticException.class)
    public void testNoTableFail() {
        DropTableStmt stmt = new DropTableStmt(false, new TableName("db1", ""), true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.fail("No Exception throws.");
    }

}