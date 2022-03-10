// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/StmtExecutorTest.java

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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.KillStmt;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.ShowAuthorStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.SqlParser;
import com.starrocks.analysis.UseStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TUniqueId;
import java_cup.runtime.Symbol;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class StmtExecutorTest {
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;

    @Mocked
    SocketChannel socketChannel;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        state = new QueryState();
        scheduler = new ConnectScheduler(10);
        ctx = new ConnectContext(socketChannel);

        ctx.setQueryId(UUIDUtil.genUUID());

        SessionVariable sessionVariable = new SessionVariable();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Catalog catalog = AccessTestUtil.fetchAdminCatalog();

        MysqlChannel channel = new MysqlChannel(socketChannel);
        new Expectations(channel) {
            {
                channel.sendOnePacket((ByteBuffer) any);
                minTimes = 0;

                channel.reset();
                minTimes = 0;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getMysqlChannel();
                minTimes = 0;
                result = channel;

                ctx.getSerializer();
                minTimes = 0;
                result = serializer;

                ctx.getCatalog();
                minTimes = 0;
                result = catalog;

                ctx.getState();
                minTimes = 0;
                result = state;

                ctx.getConnectScheduler();
                minTimes = 0;
                result = scheduler;

                ctx.getConnectionId();
                minTimes = 0;
                result = 1;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                ctx.getForwardedStmtId();
                minTimes = 0;
                result = 123L;

                ctx.setKilled();
                minTimes = 0;

                ctx.updateReturnRows(anyInt);
                minTimes = 0;

                ctx.setExecutionId((TUniqueId) any);
                minTimes = 0;

                ctx.getExecutionId();
                minTimes = 0;
                result = new TUniqueId();

                ctx.getStartTime();
                minTimes = 0;
                result = 0L;

                ctx.getDatabase();
                minTimes = 0;
                result = "testCluster:testDb";

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                ctx.setStmtId(anyLong);
                minTimes = 0;

                ctx.getStmtId();
                minTimes = 0;
                result = 1L;
            }
        };
    }

    @Test
    public void testShow(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor)
            throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt();
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = new ShowResultSet(new ShowAuthorStmt().getMetaData(), rows);
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShowNull(@Mocked ShowStmt showStmt, @Mocked SqlParser parser, @Mocked ShowExecutor executor)
            throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt();
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKill(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = ctx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKillOtherFail(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx)
            throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();

        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getCatalog();
                minTimes = 0;
                result = killCatalog;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "blockUser";

                killCtx.kill(true);
                minTimes = 0;

                killCtx.getQueryId();
                result = UUIDUtil.genUUID();

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillOther(@Mocked KillStmt killStmt, @Mocked SqlParser parser, @Mocked ConnectContext killCtx)
            throws Exception {
        Catalog killCatalog = AccessTestUtil.fetchAdminCatalog();
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getCatalog();
                minTimes = 0;
                result = killCatalog;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "killUser";

                killCtx.kill(true);
                minTimes = 0;

                killCtx.getQueryId();
                result = UUIDUtil.genUUID();

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1L);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillNoCtx(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                scheduler.getContext(1L);
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testSet(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor)
            throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testSetFail(@Mocked SetStmt setStmt, @Mocked SqlParser parser, @Mocked SetExecutor executor)
            throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
                result = new DdlException("failed");
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdl(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testDdlFail(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
                result = new DdlException("ddl fail");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdlFail2(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Catalog) any, (DdlStmt) any);
                minTimes = 0;
                result = new Exception("bug");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testUse(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "testCluster:testDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                minTimes = 0;
                result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testUseFail(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "blockDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getClusterName();
                minTimes = 0;
                result = "testCluster";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }
}

