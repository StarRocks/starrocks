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

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.proc.BrokerProcNode;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowBrokerStmt;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class BrokerMgrTest {
    @BeforeEach
    public void setUp() throws Exception {
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        BrokerMgr brokerMgr = new BrokerMgr();
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        brokerMgr.addBrokers("HDFS", addresses);

        FsBroker broker = brokerMgr.getBroker("HDFS", "127.0.0.1", 8080);
        broker.lastStartTime = System.currentTimeMillis();
        broker.lastUpdateTime = System.currentTimeMillis();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getBrokerMgr();
                minTimes = 0;
                result = brokerMgr;
            }
        };
    }

    @Test
    public void testIPTitle() {
        Assertions.assertEquals("IP", BrokerProcNode.BROKER_PROC_NODE_TITLE_NAMES.get(1));
    }

    @Test
    public void test() throws DdlException {
        BrokerMgr brokerMgr = GlobalStateMgr.getCurrentState().getBrokerMgr();
        String json = GsonUtils.GSON.toJson(brokerMgr);

        BrokerMgr replayBrokerMgr = GsonUtils.GSON.fromJson(json, BrokerMgr.class);
        Assertions.assertNotNull(replayBrokerMgr.getBroker("HDFS", "127.0.0.1", 8080));
    }

    @Test
    public void testShowBrokers() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionContext = new SessionVariable();
        sessionContext.setTimeZone("UTC");
        connectContext.setSessionVariable(sessionContext);
        connectContext.setThreadLocalInfo();

        ShowBrokerStmt stmt = new ShowBrokerStmt();
        ShowResultSet showResultSet = ShowExecutor.execute(stmt, connectContext);
        Assertions.assertEquals("HDFS", showResultSet.getResultRows().get(0).get(0));
    }
}
