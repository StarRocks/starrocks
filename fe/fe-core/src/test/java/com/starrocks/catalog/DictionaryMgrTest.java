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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.DictionaryMgrInfo;
import com.starrocks.proto.PProcessDictionaryCacheResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DictionaryMgrTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    private Backend aliveBackend;
    private Backend deadBackend;
    private ComputeNode aliveComputeNode;
    private ComputeNode deadComputeNode;

    @BeforeEach
    public void setUp() {
        aliveBackend = new Backend(1, "backend-ok", 9050);
        aliveBackend.setBrpcPort(8060);
        aliveBackend.setAlive(true);

        deadBackend = new Backend(2, "backend-dead", 9051);
        deadBackend.setBrpcPort(0);

        aliveComputeNode = new ComputeNode(3, "compute-ok", 9052);
        aliveComputeNode.setBrpcPort(9060);
        aliveComputeNode.setAlive(true);

        deadComputeNode = new ComputeNode(4, "compute-dead", 9053);
        deadComputeNode.setBrpcPort(0);

        final GlobalStateMgr mockedStateMgr = globalStateMgr;
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return mockedStateMgr;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };
    }

    @Test
    public void testGetBeOrCn() throws Exception {
        new Expectations() {
            {
                systemInfoService.getBackends();
                result = Arrays.asList(aliveBackend, deadBackend);

                systemInfoService.getComputeNodes();
                result = Arrays.asList(aliveComputeNode, deadComputeNode);
            }
        };

        List<TNetworkAddress> nodes = Lists.newArrayList();
        DictionaryMgr.fillBackendsOrComputeNodes(nodes);

        Assertions.assertEquals(2, nodes.size());
        Assertions.assertTrue(nodes.stream().anyMatch(addr -> addr.getHostname().equals("backend-ok")));
        Assertions.assertTrue(nodes.stream().anyMatch(addr -> addr.getHostname().equals("compute-ok")));
        Assertions.assertFalse(nodes.stream().anyMatch(addr -> addr.getHostname().equals("backend-dead")));
        Assertions.assertFalse(nodes.stream().anyMatch(addr -> addr.getHostname().equals("compute-dead")));
    }

    @Test
    public void testShowDictionary() throws Exception {
        DictionaryMgr mgr = new DictionaryMgr();
        List<String> dictionaryKeys = Lists.newArrayList("key");
        List<String> dictionaryValues = Lists.newArrayList("value");
        Dictionary dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, new HashMap<>());
        dictionary.setLastSuccessVersion(System.currentTimeMillis());
        mgr.addDictionary(dictionary);

        ConnectContext context = ConnectContext.buildInner();
        context.setThreadLocalInfo();

        Map<TNetworkAddress, DictionaryMgr.DictionaryCacheNodeStatistic> stats = new LinkedHashMap<>();
        PProcessDictionaryCacheResult availableResult = new PProcessDictionaryCacheResult();
        availableResult.dictionaryMemoryUsage = 1024L;
        stats.put(new TNetworkAddress("backend-ok", 8060),
                new DictionaryMgr.DictionaryCacheNodeStatistic(availableResult, null));
        stats.put(new TNetworkAddress("backend-offline", 8061),
                new DictionaryMgr.DictionaryCacheNodeStatistic(null, "Unavailable"));

        new MockUp<TimeUtils>() {
            @Mock
            public String longToTimeString(long timeStamp) {
                return "mock-time";
            }

            @Mock
            public String longToTimeString(long timeStamp, java.text.SimpleDateFormat dateFormat) {
                return "mock-time";
            }
        };

        new MockUp<DictionaryMgr>() {
            @Mock
            public Map<TNetworkAddress, DictionaryMgr.DictionaryCacheNodeStatistic> getDictionaryStatistic(
                    Dictionary dict) {
                return stats;
            }
        };

        List<List<String>> info = mgr.getAllInfo(null);
        Assertions.assertEquals(1, info.size());
        String memoryInfo = info.get(0).get(info.get(0).size() - 1);
        Assertions.assertTrue(memoryInfo.contains("backend-ok:8060"));
        Assertions.assertTrue(memoryInfo.contains("1024"));
        Assertions.assertTrue(memoryInfo.contains("backend-offline:8061"));
        Assertions.assertTrue(memoryInfo.contains("Unavailable"));
    }

    @Test
    public void testResetStateFunction() throws Exception {
        Dictionary dictionary = new Dictionary();
        dictionary.resetState();
    }

    @Test
    public void testFollower() throws Exception {
        new Expectations() {
            {
                globalStateMgr.isLeader();
                minTimes = 0;
                result = false;
            }
        };

        Dictionary dictionary = new Dictionary();
        List<Dictionary> dictionaries = Lists.newArrayList();
        dictionaries.add(dictionary);

        DictionaryMgrInfo dictionaryMgrInfo = new DictionaryMgrInfo(1, 1, dictionaries);

        DictionaryMgr dictionaryMgr = new DictionaryMgr();
        dictionaryMgr.syncDictionaryMeta(dictionaries);
        dictionaryMgr.scheduleTasks();
        dictionaryMgr.replayModifyDictionaryMgr(dictionaryMgrInfo);
    }
}
