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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TConnectionInfo;
import com.starrocks.thrift.TListConnectionResponse;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowProcesslistStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        testSuccess("SHOW PROCESSLIST");
        testSuccess("SHOW FULL PROCESSLIST");
    }

    private void testSuccess(String originStmt) throws Exception {
        ShowProcesslistStmt stmt = (ShowProcesslistStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(11, metaData.getColumnCount());
        Assert.assertEquals("ServerName", metaData.getColumn(0).getName());
        Assert.assertEquals("Id", metaData.getColumn(1).getName());
        Assert.assertEquals("User", metaData.getColumn(2).getName());
        Assert.assertEquals("Host", metaData.getColumn(3).getName());
        Assert.assertEquals("Db", metaData.getColumn(4).getName());
        Assert.assertEquals("Command", metaData.getColumn(5).getName());
        Assert.assertEquals("ConnectionStartTime", metaData.getColumn(6).getName());
        Assert.assertEquals("Time", metaData.getColumn(7).getName());
        Assert.assertEquals("State", metaData.getColumn(8).getName());
        Assert.assertEquals("Info", metaData.getColumn(9).getName());
        Assert.assertEquals("IsPending", metaData.getColumn(10).getName());
    }

    @Test
    public void testRemoteFeConnection() {

        ShowProcesslistStmt showProcesslistStmt = new ShowProcesslistStmt(false);

        Frontend frontend1 = new Frontend(1, FrontendNodeType.LEADER, "127.0.0.1", "127.0.0.1", 9030);
        Frontend frontend2 = new Frontend(2, FrontendNodeType.FOLLOWER, "127.0.0.2", "127.0.0.2", 9030);
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return frontend1;
            }

            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                return Lists.newArrayList(frontend1, frontend2);
            }
        };


        ConnectContext ctx1 = new ConnectContext();
        ctx1.setQualifiedUser("test");
        ctx1.setCurrentUserIdentity(new UserIdentity("test", "%"));
        ctx1.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx1.setConnectionId(1);

        ConnectContext ctx2 = new ConnectContext();
        ctx2.setQualifiedUser("test2");
        ctx2.setCurrentUserIdentity(new UserIdentity("test2", "%"));
        ctx2.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx2.setConnectionId(2);

        ExecuteEnv.setup();
        ExecuteEnv.getInstance().getScheduler().registerConnection(ctx1);
        ExecuteEnv.getInstance().getScheduler().registerConnection(ctx2);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);

        TListConnectionResponse tListConnectionResponse = new TListConnectionResponse();
        TConnectionInfo tConnectionInfo = new TConnectionInfo();
        tConnectionInfo.setConnection_id("3");
        tConnectionInfo.setUser("user");
        tConnectionInfo.setHost("127.0.0.1");
        tConnectionInfo.setDb("");
        tConnectionInfo.setCommand("command");
        tConnectionInfo.setConnection_start_time("");
        tConnectionInfo.setTime("10");
        tConnectionInfo.setState("OK");
        tConnectionInfo.setInfo("info");
        tConnectionInfo.setIsPending("false");
        tListConnectionResponse.addToConnections(tConnectionInfo);

        try (MockedStatic<ThriftRPCRequestExecutor> thriftConnectionPoolMockedStatic =
                Mockito.mockStatic(ThriftRPCRequestExecutor.class)) {
            thriftConnectionPoolMockedStatic.when(()
                            -> ThriftRPCRequestExecutor.call(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(tListConnectionResponse);
            ShowResultSet showResultSet = ShowExecutor.execute(showProcesslistStmt, context);
            Assert.assertEquals(3, showResultSet.getResultRows().size());
        }
    }
}
