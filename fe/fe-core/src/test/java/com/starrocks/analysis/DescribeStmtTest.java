// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DescribeStmtTest.java

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
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class DescribeStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("testDb").useDatabase("testTbl");
        starRocksAssert.withTable("create table testDb.testTbl(" +
                "id int, name string) DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                "PROPERTIES(\"replication_num\" = \"1\")");
    }

    @Test
    public void testDescribeTable() {
        DescribeStmt stmt = new DescribeStmt(new TableName("testDb", "testTbl"), false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("DESCRIBE `testDb.testTbl`", stmt.toString());
        Assert.assertEquals(6, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());

        DescribeStmt stmtAll = new DescribeStmt(new TableName("testDb", "testTbl"), true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtAll, ctx);
        Assert.assertEquals("DESCRIBE `testDb.testTbl` ALL", stmtAll.toString());
        Assert.assertEquals(6, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());
    }

    @Test
    public void testDescExplain() throws UserException {
        String sql = "desc insert into testDb.testTbl select * from testDb.testTbl";
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, ctx);
        Assert.assertTrue(((InsertStmt) statementBase).getQueryStatement().isExplain());
    }
}
