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

import com.google.common.collect.Lists;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DictionaryMgrTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    private List<Backend> backends = Arrays.asList(new Backend(1, "127.0.0.1", 1234));
    private List<ComputeNode> computeNodes = Arrays.asList(new ComputeNode(2, "127.0.0.2", 1235));

    private DictionaryMgr dictionaryMgr = new DictionaryMgr();

    @Before
    public void setUp() {
        new Expectations() {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.isReady();
                minTimes = 0;
                result = true;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations() {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getBackends();
                minTimes = 0;
                result = backends;

                systemInfoService.getComputeNodes();
                minTimes = 0;
                result = computeNodes;
            }
        };
    }

    @Test
    public void testGetBeOrCn() throws Exception {
        List<TNetworkAddress> nodes = Lists.newArrayList();
        dictionaryMgr.fillBackendsOrComputeNodes(nodes);
    }
}
