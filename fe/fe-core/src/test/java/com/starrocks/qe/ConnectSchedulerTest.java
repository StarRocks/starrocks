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
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlProto;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

public class ConnectSchedulerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectScheduler.class);
    @Mocked
    SocketChannel socketChannel;
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

                // mock negotiate
                MysqlProto.negotiate((ConnectContext) any);
                minTimes = 0;
                result = new MysqlProto.NegotiateResult(null, true);

                MysqlProto.sendResponsePacket((ConnectContext) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testProcessException(@Mocked ConnectProcessor processor) throws Exception {
        ConnectScheduler scheduler = new ConnectScheduler(10);

        ConnectContext context = new ConnectContext(socketChannel);
        context.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        context.setQualifiedUser("root");
        Assert.assertTrue(scheduler.submit(context));
        Assert.assertEquals(0, context.getConnectionId());

        Thread.sleep(1000);
        Assert.assertNull(scheduler.getContext(0));
    }

    @Test
    public void testSubmitTooMany() throws InterruptedException {
        ConnectScheduler scheduler = new ConnectScheduler(0);
        ConnectContext context = new ConnectContext(socketChannel);
        Assert.assertTrue(scheduler.submit(context));
    }
}
