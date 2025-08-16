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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TConnectionInfo;
import com.starrocks.thrift.TListConnectionResponse;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowProcesslistStmtTest {
    private static ConnectContext connectContext;

    @BeforeAll
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
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertNotNull(metaData);
        Assertions.assertEquals(13, metaData.getColumnCount());
        Assertions.assertEquals("ServerName", metaData.getColumn(0).getName());
        Assertions.assertEquals("Id", metaData.getColumn(1).getName());
        Assertions.assertEquals("User", metaData.getColumn(2).getName());
        Assertions.assertEquals("Host", metaData.getColumn(3).getName());
        Assertions.assertEquals("Db", metaData.getColumn(4).getName());
        Assertions.assertEquals("Command", metaData.getColumn(5).getName());
        Assertions.assertEquals("ConnectionStartTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("Time", metaData.getColumn(7).getName());
        Assertions.assertEquals("State", metaData.getColumn(8).getName());
        Assertions.assertEquals("Info", metaData.getColumn(9).getName());
        Assertions.assertEquals("IsPending", metaData.getColumn(10).getName());
        Assertions.assertEquals("Warehouse", metaData.getColumn(11).getName());
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
        String cnGroupName = "cngroup_0";
        new MockUp<ConnectContext>() {
            @Mock
            public String getCurrentComputeResourceName() {
                return cnGroupName;
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
        tConnectionInfo.setWarehouse("default_warehouse");
        tConnectionInfo.setCngroup(cnGroupName);
        tListConnectionResponse.addToConnections(tConnectionInfo);

        try (MockedStatic<ThriftRPCRequestExecutor> thriftConnectionPoolMockedStatic =
                Mockito.mockStatic(ThriftRPCRequestExecutor.class)) {
            thriftConnectionPoolMockedStatic.when(()
                            -> ThriftRPCRequestExecutor.call(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(tListConnectionResponse);
            ShowResultSet showResultSet = ShowExecutor.execute(showProcesslistStmt, context);
            Assertions.assertEquals(3, showResultSet.getResultRows().size());

            List<List<String>> resultRows = showResultSet.getResultRows();
            Assertions.assertEquals("default_warehouse", resultRows.get(0).get(11));
            Assertions.assertEquals(cnGroupName, resultRows.get(0).get(12));
        }
    }
}
