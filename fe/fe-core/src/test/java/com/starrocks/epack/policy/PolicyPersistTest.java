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
package com.starrocks.epack.policy;

import com.starrocks.epack.persist.CreatePolicyLog;
import com.starrocks.epack.persist.DropPolicyLog;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class PolicyPersistTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        UtFrameUtils.setUpForPersistTest();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testCreateAndDrop() throws Exception {
        String sql = "create masking policy mp as ( v1 bigint) returns bigint -> v1 + 1";
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                starRocksAssert.getCtx());
        DDLStmtExecutor.execute(createPolicyStmt, starRocksAssert.getCtx());
        CreatePolicyLog createPolicyLog = (CreatePolicyLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_MASKING_POLICY);
        Assert.assertEquals("mp", createPolicyLog.getName());

        DDLStmtExecutor.execute(analyzeSuccess("drop masking policy mp"), starRocksAssert.getCtx());

        DropPolicyLog dropPolicyLog = (DropPolicyLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_DROP_POLICY);
        Assert.assertEquals("mp", dropPolicyLog.getName());
    }
}
