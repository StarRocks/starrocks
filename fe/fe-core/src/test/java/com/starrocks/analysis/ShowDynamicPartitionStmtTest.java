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

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowDynamicPartitionStmtTest {

    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");
        String showSQL = "SHOW DYNAMIC PARTITION TABLES FROM testDb";
        ShowDynamicPartitionStmt stmtFromSql =
                (ShowDynamicPartitionStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtFromSql, ctx);
        Assert.assertEquals("testDb", stmtFromSql.getDb());

        String showWithoutDbSQL = "SHOW DYNAMIC PARTITION TABLES ";
        ShowDynamicPartitionStmt stmtWithoutDbFromSql =
                (ShowDynamicPartitionStmt) UtFrameUtils.parseStmtWithNewParser(showWithoutDbSQL, ctx);
        ShowDynamicPartitionStmt stmtWithoutIndicateDb = new ShowDynamicPartitionStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtWithoutIndicateDb, ctx);
        Assert.assertEquals("testDb", stmtWithoutDbFromSql.getDb());

    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowDynamicPartitionStmt stmtWithoutDb = new ShowDynamicPartitionStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtWithoutDb, ctx);
        Assert.fail("No Exception throws.");
    }

}