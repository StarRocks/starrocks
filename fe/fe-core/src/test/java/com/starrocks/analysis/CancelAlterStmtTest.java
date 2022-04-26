// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CancelAlterStmtTest.java

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

import com.starrocks.analysis.ShowAlterStmt.AlterType;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CancelAlterStmtTest {

    private Analyzer analyzer;
    private GlobalStateMgr globalStateMgr;

    private ConnectContext ctx;

    private static FakeGlobalStateMgr fakeGlobalStateMgr;

    @Before
    public void setUp() {
        globalStateMgr = AccessTestUtil.fetchAdminCatalog();
        ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");

        analyzer = new Analyzer(globalStateMgr, ctx);
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testUser";
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        // cancel alter column
        CancelAlterTableStmt stmt = new CancelAlterTableStmt(AlterType.COLUMN, new TableName(null, "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("CANCEL ALTER COLUMN FROM `testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals(AlterType.COLUMN, stmt.getAlterType());
        Assert.assertEquals("testTbl", stmt.getTableName());

        stmt = new CancelAlterTableStmt(AlterType.ROLLUP, new TableName(null, "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("CANCEL ALTER ROLLUP FROM `testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbName());
        Assert.assertEquals(AlterType.ROLLUP, stmt.getAlterType());
    }
}
