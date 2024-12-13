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

<<<<<<< HEAD
import com.starrocks.rpc.FrontendServiceProxy;
=======
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
<<<<<<< HEAD
=======
import org.apache.spark.internal.config.R;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.channels.SocketChannel;
import java.util.UUID;

public class KillQueryHandleTest {

    private static StarRocksAssert starRocksAssert;

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testKillStmt(@Mocked SocketChannel socketChannel) throws Exception {
        // test killing query successfully
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        Assert.assertFalse(ctx1.isKilled());
        ConnectContext ctx = kill(ctx1.getQueryId().toString(), false);
        // isKilled is set
        Assert.assertTrue(ctx1.isKilled());
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

<<<<<<< HEAD
=======
    public static ConnectContext getConnectContext() {
        return connectContext;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Test
    public void testKillStmt2(@Mocked SocketChannel socketChannel, @Mocked TMasterOpResult result)
            throws Exception {
        // test killing query is forwarded to fe and query is successfully killed
<<<<<<< HEAD
        new MockUp(FrontendServiceProxy.class) {
            @Mock
            public <T> T call(TNetworkAddress address, int timeoutMs, int retryTimes,
                              FrontendServiceProxy.MethodCallable<T> callable) throws Exception {
=======
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                result.state = "OK";
                return (T) result;
            }
        };
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenQueryIdNotFound(@Mocked SocketChannel socketChannel)
            throws Exception {
        // test killing query but query not found
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        ConnectContext ctx = kill("xxx", false);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("Unknown query id: xxx", ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenQueryIdNotFound2(@Mocked SocketChannel socketChannel, @Mocked TMasterOpResult result)
            throws Exception {
        // test killing query is forwarded to fe and query not found
        new MockUp<TMasterOpResult>() {
            @Mock
            public boolean isSetErrorMsg() {
                return true;
            }

            @Mock
            public String getErrorMsg() {
                return "query xxx not found";
            }
        };
<<<<<<< HEAD
        new MockUp(FrontendServiceProxy.class) {
            @Mock
            public <T> T call(TNetworkAddress address, int timeoutMs, int retryTimes,
                              FrontendServiceProxy.MethodCallable<T> callable) throws Exception {
=======
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                result.state = "ERR";
                return (T) result;
            }
        };
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("query xxx not found", ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenConnectionFail(@Mocked SocketChannel socketChannel) throws Exception {
        // test killing query is forwarded to fe but fe is down
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
<<<<<<< HEAD
        Assert.assertEquals("Failed to connect to fe 127.0.0.1:9020", ctx.getState().getErrorMessage());
=======
        Assert.assertTrue(ctx.getState().getErrorMessage().contains("Connection refused"));
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenForwardWithUnknownError(@Mocked SocketChannel socketChannel) throws Exception {
        // test killing query is forwarded to fe with unexpected error
<<<<<<< HEAD
        new MockUp(FrontendServiceProxy.class) {
            @Mock
            public <T> T call(TNetworkAddress address, int timeoutMs, int retryTimes,
                              FrontendServiceProxy.MethodCallable<T> callable) throws Exception {
=======
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                throw new Exception("Unknown error x");
            }
        };
        ConnectContext ctx1 = prepareConnectContext(socketChannel);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("Failed to connect to fe 127.0.0.1:9020 due to Unknown error x",
                ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

<<<<<<< HEAD
    private ConnectContext prepareConnectContext(SocketChannel socketChannel) {
        ConnectContext ctx1 = new ConnectContext(socketChannel) {
            @Override
            public void kill(boolean killConnection) {
=======
    @Test
    public void testKillStmtWithCustomQueryId(@Mocked SocketChannel socketChannel) throws Exception {
        // test killing query successfully
        ConnectContext ctx1 = prepareConnectContext(socketChannel);
        ctx1.getSessionVariable().setCustomQueryId("a_custom_query_id");

        Assert.assertFalse(ctx1.isKilled());
        ConnectContext ctx = kill("a_custom_query_id", false);
        // isKilled is set
        Assert.assertTrue(ctx1.isKilled());
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    private ConnectContext prepareConnectContext(SocketChannel socketChannel) {
        ConnectContext ctx1 = new ConnectContext(socketChannel) {
            @Override
            public void kill(boolean killConnection, String cancelledMessage) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                super.isKilled = true;
            }
        };
        ctx1.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx1.setQualifiedUser("root");
        ctx1.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx1.setConnectionId(1);
        ctx1.setQueryId(UUID.randomUUID());
        ctx1.setConnectScheduler(ExecuteEnv.getInstance().getScheduler());

        ExecuteEnv.getInstance().getScheduler().registerConnection(ctx1);
        return ctx1;
    }

    private ConnectContext kill(String queryId, boolean forward) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setConnectScheduler(ExecuteEnv.getInstance().getScheduler());
        // reset state
        ctx.getState().reset();
        ctx.setForwardTimes(0);
        if (!forward) {
            ctx.setForwardTimes(1);
        }
        StatementBase killStatement =
                UtFrameUtils.parseStmtWithNewParser("kill query '" + queryId + "'", ctx);
        StmtExecutor stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), killStatement);
        stmtExecutor.execute();
        return ctx;
    }
}
