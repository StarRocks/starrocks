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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderOpExecutorTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static PseudoCluster cluster;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("d1").useDatabase("d1")
                .withTable(
                        "CREATE TABLE d1.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable(
                        "CREATE TABLE d1.t2(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testResourceGroupNameInAuditLog() throws Exception {
        String createGroup = "create resource group rg1\n" +
                "to\n" +
                "    (db='d1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '1',\n" +
                "    'mem_limit' = '50%',\n" +
                "    'concurrency_limit' = '20',\n" +
                "    'type' = 'normal'\n" +
                ");";
        cluster.runSql("d1", createGroup);

        String sql = "insert into t1 select * from t1";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        LeaderOpExecutor executor =
                new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(), connectContext, RedirectStatus.FORWARD_NO_SYNC, false,
                        null);

        mockFrontendService(new MockFrontendServiceClient());
        executor.execute();

        Assertions.assertEquals("rg1", connectContext.getAuditEventBuilder().build().resourceGroup);
    }

    private static class MockFrontendServiceClient extends FrontendService.Client {
        private final FrontendService.Iface frontendService = new FrontendServiceImpl(null);

        public MockFrontendServiceClient() {
            super(null);
        }

        @Override
        public TMasterOpResult forward(TMasterOpRequest params) throws TException {
            return frontendService.forward(params);
        }
    }

    private static void mockFrontendService(MockFrontendServiceClient client) {
        ThriftConnectionPool.frontendPool = new MockGenericPool<FrontendService.Client>("leader-op-mocked-pool") {
            @Override
            public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) {
                return client;
            }
        };
    }

    @Test
    public void testForwardTooManyTimes() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setForwardTimes(LeaderOpExecutor.MAX_FORWARD_TIMES);

        try {
            new LeaderOpExecutor(new OriginStatement("show frontends"), connectContext, RedirectStatus.FORWARD_NO_SYNC)
                    .execute();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof ErrorReportException);
            Assertions.assertEquals(ErrorCode.ERR_FORWARD_TOO_MANY_TIMES, ((ErrorReportException) e).getErrorCode());
            return;
        }
        Assertions.fail("should throw ERR_FORWARD_TOO_MANY_TIMES exception");
    }

    @Test
    public void testCreateTMasterOpRequest() {
        String catalog = "myCatalog";
        String database = "database";

        ConnectContext connectContext = new ConnectContext();
        connectContext.setGlobalStateMgr(GlobalStateMgr.getServingState());
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setCurrentRoleIds(UserIdentity.ROOT);
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setThreadLocalInfo();
        connectContext.setCurrentCatalog(catalog);
        connectContext.setDatabase(database);

        LeaderOpExecutor executor = new LeaderOpExecutor(new OriginStatement(""),
                connectContext, RedirectStatus.FORWARD_NO_SYNC);
        TMasterOpRequest request = executor.createTMasterOpRequest(connectContext, 1);
        Assertions.assertEquals(catalog, request.getCatalog());
        Assertions.assertEquals(database, request.getDb());
    }

    /**
     * A forwarded statement whose connection breaks after the leader received it must not be sent again:
     * the leader may already be running it. The leader here is a real socket that resets every connection
     * once a request has arrived, so the follower side is the real pool and the real thrift call.
     */
    @Test
    public void testForwardIsNotResentAfterTheConnectionBreaks() throws Exception {
        try (ResettingLeader leader = new ResettingLeader()) {
            ThriftConnectionPool<FrontendService.Client> savedPool = ThriftConnectionPool.frontendPool;
            ThriftConnectionPool.frontendPool = realFrontendPool();
            try {
                StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("insert into d1.t1 select * from d1.t1",
                        connectContext);
                LeaderOpExecutor executor = new LeaderOpExecutor(Pair.create("127.0.0.1", leader.port()), stmt,
                        stmt.getOrigStmt(), connectContext, RedirectStatus.FORWARD_NO_SYNC, false);
                Assertions.assertThrows(TTransportException.class, executor::execute);
                Assertions.assertEquals(1, leader.requests());
            } finally {
                ThriftConnectionPool.frontendPool = savedPool;
            }
        }
    }

    /**
     * When the connection to the leader breaks after a statement was forwarded, the client must be told
     * that the statement may have been applied, not that it failed.
     */
    @Test
    public void testBrokenForwardIsReportedAsResultUnknown() throws Exception {
        try (ResettingLeader leader = new ResettingLeader()) {
            ConnectContext ctx = executeForwarded(leader.port());
            Assertions.assertTrue(ctx.getState().isError());
            Assertions.assertEquals(ErrorCode.ERR_FORWARD_RESULT_UNKNOWN, ctx.getState().getErrorCode(),
                    ctx.getState().getErrorMessage());
            Assertions.assertEquals("08007", new String(ctx.getState().getErrorCode().getSqlState()));
            Assertions.assertTrue(ctx.getState().getErrorMessage().contains("may or may not have been applied"),
                    ctx.getState().getErrorMessage());
            Assertions.assertEquals(1, leader.requests());
        }
    }

    /**
     * A statement that could not be sent at all did not reach the leader, so it stays an ordinary failure.
     */
    @Test
    public void testForwardThatCannotConnectIsReportedAsFailed() throws Exception {
        int closedPort;
        try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            closedPort = probe.getLocalPort();
        }
        ConnectContext ctx = executeForwarded(closedPort);
        Assertions.assertTrue(ctx.getState().isError());
        Assertions.assertNotEquals(ErrorCode.ERR_FORWARD_RESULT_UNKNOWN, ctx.getState().getErrorCode());
        Assertions.assertTrue(ctx.getState().getErrorMessage().contains("Connection refused"),
                ctx.getState().getErrorMessage());
    }

    /**
     * A forward the leader answers is reported with the leader's result, not as an unknown outcome.
     */
    @Test
    public void testForwardTheLeaderAnswersReportsTheLeadersResult() throws Exception {
        FrontendService.Client client = new FrontendService.Client(null) {
            @Override
            public TMasterOpResult forward(TMasterOpRequest params) {
                TMasterOpResult result = new TMasterOpResult();
                result.setMaxJournalId(0);
                result.setState(QueryState.MysqlStateType.OK.name());
                return result;
            }
        };
        ThriftConnectionPool<FrontendService.Client> pool =
                new MockGenericPool<FrontendService.Client>("leader-op-answering-pool") {
                    @Override
                    public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) {
                        return client;
                    }
                };
        ConnectContext ctx = executeForwarded(9020, pool);
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());
        Assertions.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());
    }

    private static ConnectContext executeForwarded(int leaderPort) throws Exception {
        return executeForwarded(leaderPort, realFrontendPool());
    }

    private static ConnectContext executeForwarded(int leaderPort, ThriftConnectionPool<FrontendService.Client> pool)
            throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public boolean isForwardToLeader() {
                return true;
            }
        };
        new MockUp<NodeMgr>() {
            @Mock
            public Pair<String, Integer> getLeaderIpAndRpcPort() {
                return Pair.create("127.0.0.1", leaderPort);
            }
        };
        ThriftConnectionPool<FrontendService.Client> savedPool = ThriftConnectionPool.frontendPool;
        ThriftConnectionPool.frontendPool = pool;
        try {
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
            ctx.setThreadLocalInfo();
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("insert into d1.t1 select * from d1.t1", ctx);
            new StmtExecutor(ctx, stmt).execute();
            return ctx;
        } finally {
            ThriftConnectionPool.frontendPool = savedPool;
        }
    }

    private static ThriftConnectionPool<FrontendService.Client> realFrontendPool() {
        return new ThriftConnectionPool<>("FrontendService", new GenericKeyedObjectPoolConfig(), 5000);
    }

    /**
     * A stand-in leader that accepts connections, waits for a request to arrive, counts it and then closes
     * the connection with a reset, as a leader that dies or a network that drops the connection would.
     */
    private static class ResettingLeader implements AutoCloseable {
        private final ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        private final AtomicInteger requests = new AtomicInteger();

        ResettingLeader() throws IOException {
            Thread acceptor = new Thread(() -> {
                while (!server.isClosed()) {
                    try (Socket socket = server.accept()) {
                        if (socket.getInputStream().read(new byte[65536]) > 0) {
                            requests.incrementAndGet();
                        }
                        socket.setSoLinger(true, 0);
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "resetting-leader");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            return server.getLocalPort();
        }

        int requests() {
            return requests.get();
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }
}
