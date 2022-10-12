// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.Pair;
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
        analyzeFail("ALTER SYSTEM ADD BROKER `broker1` \"127.0.0.1\"", "broker host or port is wrong!");
    }
}