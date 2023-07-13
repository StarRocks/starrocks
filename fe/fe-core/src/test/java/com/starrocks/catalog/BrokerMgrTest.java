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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class BrokerMgrTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.PseudoImage.setUpImageVersion();
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logGlobalVariable((SessionVariable) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testIPTitle() {
        Assert.assertTrue(BrokerMgr.BROKER_PROC_NODE_TITLE_NAMES.get(1).equals("IP"));
    }

    @Test
    public void test() throws DdlException {
        BrokerMgr brokerMgr = new BrokerMgr();
        Collection<Pair<String, Integer>> addresses = new ArrayList<>();
        Pair<String, Integer> pair = new Pair<String, Integer>("127.0.0.1", 8080);
        addresses.add(pair);
        brokerMgr.addBrokers("HDFS", addresses);

        String json = GsonUtils.GSON.toJson(brokerMgr);

        BrokerMgr replayBrokerMgr = GsonUtils.GSON.fromJson(json, BrokerMgr.class);
        Assert.assertNotNull(replayBrokerMgr.getBroker("HDFS", "127.0.0.1", 8080));
    }
}
