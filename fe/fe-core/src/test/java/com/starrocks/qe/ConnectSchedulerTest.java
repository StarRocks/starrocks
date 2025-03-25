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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/ConnectSchedulerTest.java

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

import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.common.DdlException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.leader.CheckpointController;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.MockJournal;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.StreamConnection;

public class ConnectSchedulerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectScheduler.class);
    @Mocked
    StreamConnection connection;
    @Mocked
    MysqlChannel channel;
    @Mocked
    MysqlProto mysqlProto;

    @Before
    public void setUp() throws Exception {
        new Expectations() {
            {
                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";

                MysqlProto.sendResponsePacket((ConnectContext) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testProcessException(@Mocked ConnectProcessor processor) throws Exception {
        ConnectScheduler scheduler = new ConnectScheduler(10);

        ConnectContext context = new ConnectContext(connection);
        context.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        context.setQualifiedUser("root");
        context.setConnectionId(scheduler.getNextConnectionId());
        context.resetConnectionStartTime();
        Assert.assertEquals(0, context.getConnectionId());

        Thread.sleep(1000);
        Assert.assertNull(scheduler.getContext(0));
    }

    @Test
    public void testGlobalConnectionId() throws Exception {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(0, FrontendNodeType.LEADER, "", "", 0);
            }
        };

        ConnectScheduler scheduler = new ConnectScheduler(10);
        int threshold = 1 << 24;
        for (int i = 1; i < threshold; i++) {
            scheduler.getNextConnectionId();
        }

        Assert.assertEquals(0, scheduler.getNextConnectionId());

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(1, FrontendNodeType.LEADER, "", "", 0);
            }
        };
        Assert.assertEquals(threshold + 1, scheduler.getNextConnectionId());
    }

    @Test
    public void testFrontendId() throws DdlException {
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObject(short op, Object obj) {
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(null));
        GlobalStateMgr.getCurrentState().setCheckpointController(new CheckpointController("fe", new BDBJEJournal(null, ""), ""));
        GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());

        NodeMgr nodeMgr = new NodeMgr();
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0.0", 9010);
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0.1", 9010);

        Assert.assertNotNull(nodeMgr.getFrontend(0));
        Assert.assertNotNull(nodeMgr.getFrontend(1));
        Assert.assertNull(nodeMgr.getFrontend(2));
        nodeMgr.setMySelf(nodeMgr.getFrontend(0));

        for (int i = 2; i < 256; i++) {
            nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0." + i, 9010);
        }

        Frontend frontend = nodeMgr.checkFeExist("192.168.0.255", 9010);
        Assert.assertEquals(255, frontend.getFid());

        Assert.assertThrows(DdlException.class, () -> nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.20", 9010));

        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.20", 9010);
        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.50", 9010);
        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.70", 9010);

        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.20", 9010);
        frontend = nodeMgr.checkFeExist("192.168.1.20", 9010);
        Assert.assertEquals(20, frontend.getFid());
    }
}
