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

package com.starrocks.common.proc;

import com.google.common.collect.ArrayListMultimap;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrokerProcNodeTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private BrokerMgr brokerMgr;

    @Before
    public void setUp() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public BrokerMgr getBrokerMgr() {
                return brokerMgr;
            }
        };

        new MockUp<TimeUtils>() {
            @Mock
            public String longToTimeString(long timeStamp) {
                if (timeStamp <= 0L) {
                    return "N/A";
                }
                return "2023-04-15 10:30:00";
            }
        };
    }

    @Test
    public void testFetchResult() {
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = new HashMap<>();

        ArrayListMultimap<String, FsBroker> brokerMultimap = ArrayListMultimap.create();
        FsBroker broker1 = new FsBroker("192.168.1.1", 8000);
        broker1.isAlive = true;
        broker1.lastStartTime = System.currentTimeMillis();
        broker1.lastUpdateTime = System.currentTimeMillis();
        brokerMultimap.put("192.168.1.1:8000", broker1);

        FsBroker broker2 = new FsBroker("192.168.1.2", 8000);
        broker2.isAlive = false;
        broker2.lastStartTime = System.currentTimeMillis() - 3600 * 1000;
        broker2.lastUpdateTime = System.currentTimeMillis() - 1800 * 1000;
        broker2.heartbeatErrMsg = "Connection timeout";
        brokerMultimap.put("192.168.1.2:8000", broker2);

        brokersMap.put("hdfs", brokerMultimap);

        new MockUp<BrokerMgr>() {
            @Mock
            public Map<String, ArrayListMultimap<String, FsBroker>> getBrokersMap() {
                return brokersMap;
            }
        };

        BrokerProcNode node = new BrokerProcNode();
        ProcResult result = node.fetchResult();

        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(BrokerProcNode.BROKER_PROC_NODE_TITLE_NAMES, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(2, rows.size());

        List<String> row1 = rows.get(0);
        Assert.assertEquals(7, row1.size());
        Assert.assertEquals("hdfs", row1.get(0));
        Assert.assertEquals("192.168.1.1", row1.get(1));
        Assert.assertEquals("8000", row1.get(2));
        Assert.assertEquals("true", row1.get(3));
        Assert.assertEquals("2023-04-15 10:30:00", row1.get(4));
        Assert.assertEquals("2023-04-15 10:30:00", row1.get(5));

        List<String> row2 = rows.get(1);
        Assert.assertEquals(7, row2.size());
        Assert.assertEquals("hdfs", row2.get(0));
        Assert.assertEquals("192.168.1.2", row2.get(1));
        Assert.assertEquals("8000", row2.get(2));
        Assert.assertEquals("false", row2.get(3));
        Assert.assertEquals("2023-04-15 10:30:00", row2.get(4));
        Assert.assertEquals("2023-04-15 10:30:00", row2.get(5));
        Assert.assertEquals("Connection timeout", row2.get(6));
    }
} 