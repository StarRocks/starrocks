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

import com.google.common.collect.ImmutableSet;
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.structure.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;

public class ModifyBrokerClauseTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAddBrokerCluase() throws Exception {
        String brokerName = "broker1";
        String hostPort = "127.0.0.1:8000";
        String stmt = String.format("ALTER SYSTEM ADD BROKER %s \"%s\"", brokerName, hostPort);
        ModifyBrokerClause clause = (ModifyBrokerClause)(
                (AlterSystemStmt)UtFrameUtils.parseStmtWithNewParser(stmt, connectContext)).getAlterClause();
        Assert.assertEquals(ModifyBrokerClause.ModifyOp.OP_ADD, clause.getOp());
        Assert.assertEquals(AlterOpType.ALTER_OTHER, clause.getOpType());
        Assert.assertEquals(brokerName, clause.getBrokerName());
        Assert.assertEquals(ImmutableSet.of(Pair.create("127.0.0.1", 8000)), clause.getHostPortPairs());
    }

    @Test
    public void testDropBrokerCluase() throws Exception {
        String brokerName = "broker1";
        String hostPort1 = "127.0.0.1:8000";
        String hostPort2 = "127.0.0.1:8001";
        String stmt = String.format("ALTER SYSTEM DROP BROKER %s \"%s\", \"%s\"", brokerName, hostPort1, hostPort2);
        ModifyBrokerClause clause = (ModifyBrokerClause)(
                (AlterSystemStmt)UtFrameUtils.parseStmtWithNewParser(stmt, connectContext)).getAlterClause();
        Assert.assertEquals(ModifyBrokerClause.ModifyOp.OP_DROP, clause.getOp());
        Assert.assertEquals(AlterOpType.ALTER_OTHER, clause.getOpType());
        Assert.assertEquals(brokerName, clause.getBrokerName());
        Assert.assertEquals(ImmutableSet.of(Pair.create("127.0.0.1", 8000), Pair.create("127.0.0.1", 8001)), clause.getHostPortPairs());
    }

    @Test
    public void testDropAllBrokerCluase() throws Exception {
        String brokerName = "broker1";
        String stmt = String.format("ALTER SYSTEM DROP ALL BROKER %s", brokerName);
        ModifyBrokerClause clause = (ModifyBrokerClause)(
                (AlterSystemStmt)UtFrameUtils.parseStmtWithNewParser(stmt, connectContext)).getAlterClause();
        Assert.assertEquals(ModifyBrokerClause.ModifyOp.OP_DROP_ALL, clause.getOp());
        Assert.assertEquals(AlterOpType.ALTER_OTHER, clause.getOpType());
        Assert.assertEquals(brokerName, clause.getBrokerName());
        Assert.assertTrue(clause.getHostPortPairs().isEmpty());
    }

    @Test
    public void testNormal() throws Exception {
        analyzeFail("ALTER SYSTEM ADD BROKER `` \"127.0.0.1:8000\"", "Broker's name can't be empty.");
        analyzeFail("ALTER SYSTEM ADD BROKER `broker1` \"127.0.0.1\"", "BROKER host or port is wrong.");
    }
}