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

package com.starrocks.qe;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class LeaderOpExecutorStarMgrJournalTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);

        FeConstants.runningUnitTest = true;
        connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("d1").useDatabase("d1")
                .withTable("CREATE TABLE d1.t1(k1 int, k2 int, k3 int)" +
                        " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    private GlobalStateMgr mockGlobalStateMgrAsFollower() {
        GlobalStateMgr mockGsm = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(mockGsm.isLeader()).thenReturn(false);
        JournalObservable feObservable = Mockito.mock(JournalObservable.class);
        Mockito.when(mockGsm.getJournalObservable()).thenReturn(feObservable);
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getLeaderIpAndRpcPort())
                .thenReturn(Pair.create("127.0.0.1", 9020));
        Mockito.when(mockGsm.getNodeMgr()).thenReturn(mockNodeMgr);
        return mockGsm;
    }

    @Test
    public void testExecute_sharedDataMode_waitsForStarMgrJournal() throws Exception {
        String sql = "begin";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        TMasterOpResult tMasterOpResult = new TMasterOpResult();
        tMasterOpResult.setMaxJournalId(100L);
        tMasterOpResult.setMaxStarMgrJournalId(200L);

        GlobalStateMgr mockGsm = mockGlobalStateMgrAsFollower();
        StarMgrServer mockStarMgrServer = Mockito.mock(StarMgrServer.class);
        JournalObservable mockStarMgrObservable = Mockito.mock(JournalObservable.class);
        Mockito.when(mockStarMgrServer.getStarMgrJournalObservable()).thenReturn(mockStarMgrObservable);
        GlobalStateMgr originalGsm = connectContext.getGlobalStateMgr();

        try (MockedStatic<ThriftRPCRequestExecutor> thriftMock =
                        Mockito.mockStatic(ThriftRPCRequestExecutor.class);
                MockedStatic<GlobalStateMgr> gsmMock =
                        Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> runModeMock =
                        Mockito.mockStatic(RunMode.class);
                MockedStatic<StarMgrServer> starMgrMock =
                        Mockito.mockStatic(StarMgrServer.class)) {
            thriftMock.when(() -> ThriftRPCRequestExecutor.call(
                    Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(tMasterOpResult);
            gsmMock.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            runModeMock.when(RunMode::isSharedDataMode).thenReturn(true);
            starMgrMock.when(StarMgrServer::getCurrentState).thenReturn(mockStarMgrServer);

            connectContext.setGlobalStateMgr(mockGsm);
            LeaderOpExecutor executor = new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(),
                    connectContext, RedirectStatus.FORWARD_NO_SYNC, false, null);
            executor.execute();

            Mockito.verify(mockStarMgrObservable).waitOn(
                    Mockito.eq(200L), Mockito.anyInt(), Mockito.any());
        } finally {
            connectContext.setGlobalStateMgr(originalGsm);
        }
    }

    @Test
    public void testExecute_sharedDataMode_starMgrAlreadyCaughtUp() throws Exception {
        String sql = "begin";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        TMasterOpResult tMasterOpResult = new TMasterOpResult();
        tMasterOpResult.setMaxJournalId(100L);
        tMasterOpResult.setMaxStarMgrJournalId(50L);

        GlobalStateMgr mockGsm = mockGlobalStateMgrAsFollower();
        StarMgrServer mockStarMgrServer = Mockito.mock(StarMgrServer.class);
        JournalObservable realObservable = new JournalObservable();
        Mockito.when(mockStarMgrServer.getStarMgrJournalObservable()).thenReturn(realObservable);
        Mockito.when(mockStarMgrServer.getReplayId()).thenReturn(100L);
        GlobalStateMgr originalGsm = connectContext.getGlobalStateMgr();

        try (MockedStatic<ThriftRPCRequestExecutor> thriftMock =
                        Mockito.mockStatic(ThriftRPCRequestExecutor.class);
                MockedStatic<GlobalStateMgr> gsmMock =
                        Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> runModeMock =
                        Mockito.mockStatic(RunMode.class);
                MockedStatic<StarMgrServer> starMgrMock =
                        Mockito.mockStatic(StarMgrServer.class)) {

            thriftMock.when(() -> ThriftRPCRequestExecutor.call(
                    Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(tMasterOpResult);
            gsmMock.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            runModeMock.when(RunMode::isSharedDataMode).thenReturn(true);
            starMgrMock.when(StarMgrServer::getCurrentState).thenReturn(mockStarMgrServer);

            long start = System.currentTimeMillis();
            connectContext.setGlobalStateMgr(mockGsm);
            LeaderOpExecutor executor = new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(),
                    connectContext, RedirectStatus.FORWARD_NO_SYNC, false, null);
            executor.execute();
            long elapsed = System.currentTimeMillis() - start;

            Assertions.assertTrue(elapsed < 2000, "Expected fast return but took " + elapsed + "ms");
        } finally {
            connectContext.setGlobalStateMgr(originalGsm);
        }
    }

    @Test
    public void testExecute_notSharedDataMode_skipsStarMgrWait() throws Exception {
        String sql = "begin";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        TMasterOpResult tMasterOpResult = new TMasterOpResult();
        tMasterOpResult.setMaxJournalId(100L);
        tMasterOpResult.setMaxStarMgrJournalId(200L);

        GlobalStateMgr mockGsm = mockGlobalStateMgrAsFollower();

        try (MockedStatic<ThriftRPCRequestExecutor> thriftMock =
                        Mockito.mockStatic(ThriftRPCRequestExecutor.class);
                MockedStatic<GlobalStateMgr> gsmMock =
                        Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> runModeMock =
                        Mockito.mockStatic(RunMode.class);
                MockedStatic<StarMgrServer> starMgrMock =
                        Mockito.mockStatic(StarMgrServer.class)) {

            thriftMock.when(() -> ThriftRPCRequestExecutor.call(
                    Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(tMasterOpResult);
            gsmMock.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            runModeMock.when(RunMode::isSharedDataMode).thenReturn(false);

            LeaderOpExecutor executor = new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(),
                    connectContext, RedirectStatus.FORWARD_NO_SYNC, false, null);
            executor.execute();

            starMgrMock.verify(StarMgrServer::getCurrentState, Mockito.never());
        }
    }

    @Test
    public void testExecute_starMgrJournalIdNotSet_backwardCompat() throws Exception {
        String sql = "begin";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        TMasterOpResult tMasterOpResult = new TMasterOpResult();
        tMasterOpResult.setMaxJournalId(100L);

        GlobalStateMgr mockGsm = mockGlobalStateMgrAsFollower();

        try (MockedStatic<ThriftRPCRequestExecutor> thriftMock =
                        Mockito.mockStatic(ThriftRPCRequestExecutor.class);
                MockedStatic<GlobalStateMgr> gsmMock =
                        Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> runModeMock =
                        Mockito.mockStatic(RunMode.class);
                MockedStatic<StarMgrServer> starMgrMock =
                        Mockito.mockStatic(StarMgrServer.class)) {

            thriftMock.when(() -> ThriftRPCRequestExecutor.call(
                    Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(tMasterOpResult);
            gsmMock.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            runModeMock.when(RunMode::isSharedDataMode).thenReturn(true);

            LeaderOpExecutor executor = new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(),
                    connectContext, RedirectStatus.FORWARD_NO_SYNC, false, null);
            executor.execute();

            starMgrMock.verify(StarMgrServer::getCurrentState, Mockito.never());
        }
    }

    @Test
    public void testExecute_sharedDataMode_starMgrTimeout() throws Exception {
        String sql = "begin";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        TMasterOpResult tMasterOpResult = new TMasterOpResult();
        tMasterOpResult.setMaxJournalId(100L);
        tMasterOpResult.setMaxStarMgrJournalId(99999L);

        GlobalStateMgr mockGsm = mockGlobalStateMgrAsFollower();
        StarMgrServer mockStarMgrServer = Mockito.mock(StarMgrServer.class);
        JournalObservable realObservable = new JournalObservable();
        Mockito.when(mockStarMgrServer.getStarMgrJournalObservable()).thenReturn(realObservable);
        Mockito.when(mockStarMgrServer.getReplayId()).thenReturn(1L);

        connectContext.getSessionVariable().setQueryTimeoutS(2);

        try (MockedStatic<ThriftRPCRequestExecutor> thriftMock =
                        Mockito.mockStatic(ThriftRPCRequestExecutor.class);
                MockedStatic<GlobalStateMgr> gsmMock =
                        Mockito.mockStatic(GlobalStateMgr.class);
                MockedStatic<RunMode> runModeMock =
                        Mockito.mockStatic(RunMode.class);
                MockedStatic<StarMgrServer> starMgrMock =
                        Mockito.mockStatic(StarMgrServer.class)) {

            thriftMock.when(() -> ThriftRPCRequestExecutor.call(
                    Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any()))
                    .thenReturn(tMasterOpResult);
            gsmMock.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            runModeMock.when(RunMode::isSharedDataMode).thenReturn(true);
            starMgrMock.when(StarMgrServer::getCurrentState).thenReturn(mockStarMgrServer);

            LeaderOpExecutor executor = new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(),
                    connectContext, RedirectStatus.FORWARD_WITH_SYNC, false, null);

            Assertions.assertThrows(DdlException.class, executor::execute);
        } finally {
            connectContext.getSessionVariable().setQueryTimeoutS(300);
        }
    }
}
