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

import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
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
import org.apache.spark.internal.config.R;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xnio.StreamConnection;

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
    public void testKillStmt(@Mocked StreamConnection connection) throws Exception {
        // test killing query successfully
        ConnectContext ctx1 = prepareConnectContext(connection);

        Assert.assertFalse(ctx1.isKilled());
        ConnectContext ctx = kill(ctx1.getQueryId().toString(), false);
        // isKilled is set
        Assert.assertTrue(ctx1.isKilled());
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    public static ConnectContext getConnectContext() {
        return connectContext;
    }

    @Test
    public void testKillStmt2(@Mocked StreamConnection connection, @Mocked TMasterOpResult result)
            throws Exception {
        // test killing query is forwarded to fe and query is successfully killed
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
                result.state = "OK";
                return (T) result;
            }
        };
        ConnectContext ctx1 = prepareConnectContext(connection);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenQueryIdNotFound(@Mocked StreamConnection connection)
            throws Exception {
        // test killing query but query not found
        ConnectContext ctx1 = prepareConnectContext(connection);

        ConnectContext ctx = kill("xxx", false);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("Unknown query id: xxx", ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenQueryIdNotFound2(@Mocked StreamConnection connection, @Mocked TMasterOpResult result)
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
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
                result.state = "ERR";
                return (T) result;
            }
        };
        ConnectContext ctx1 = prepareConnectContext(connection);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("query xxx not found", ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenConnectionFail(@Mocked StreamConnection connection) throws Exception {
        // test killing query is forwarded to fe but fe is down
        ConnectContext ctx1 = prepareConnectContext(connection);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertTrue(ctx.getState().getErrorMessage().contains("Connection refused"));

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWhenForwardWithUnknownError(@Mocked StreamConnection connection) throws Exception {
        // test killing query is forwarded to fe with unexpected error
        new MockUp(ThriftRPCRequestExecutor.class) {
            @Mock
            public <T, SERVER_CLIENT extends org.apache.thrift.TServiceClient> T call(
                    ThriftConnectionPool<SERVER_CLIENT> genericPool,
                    TNetworkAddress address, int timeoutMs, int retryTimes,
                    ThriftRPCRequestExecutor.MethodCallable<T, R> callable) throws Exception {
                throw new Exception("Unknown error x");
            }
        };
        ConnectContext ctx1 = prepareConnectContext(connection);

        ConnectContext ctx = kill(ctx1.getQueryId().toString(), true);
        Assert.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assert.assertEquals("Failed to connect to fe 127.0.0.1:9020 due to Unknown error x",
                ctx.getState().getErrorMessage());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    @Test
    public void testKillStmtWithCustomQueryId(@Mocked StreamConnection connection) throws Exception {
        // test killing query successfully
        ConnectContext ctx1 = prepareConnectContext(connection);
        ctx1.getSessionVariable().setCustomQueryId("a_custom_query_id");

        Assert.assertFalse(ctx1.isKilled());
        ConnectContext ctx = kill("a_custom_query_id", false);
        // isKilled is set
        Assert.assertTrue(ctx1.isKilled());
        Assert.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());

        ctx1.getConnectScheduler().unregisterConnection(ctx1);
    }

    private ConnectContext prepareConnectContext(StreamConnection connection) {
        ConnectContext ctx1 = new ConnectContext(connection) {
            @Override
            public void kill(boolean killConnection, String cancelledMessage) {
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
