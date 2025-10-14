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
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.DictionaryMgrInfo;
import com.starrocks.proto.PProcessDictionaryCacheRequest;
import com.starrocks.proto.PProcessDictionaryCacheResult;
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
<<<<<<< HEAD
import org.junit.Before;
import org.junit.Test;
=======
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
>>>>>>> 15f0a2edcf ([BugFix] Fix incorrect errmsg when dictionary has error (#63841))

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key");
        dictionaryValues.add("value");
        Dictionary dictionary =
                    new Dictionary(1, "dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);
        Map<Long, Dictionary> dictionariesMapById = new HashMap<>();
        dictionariesMapById.put(1L, dictionary);

        Map<TNetworkAddress, PProcessDictionaryCacheResult> resultMap = new HashMap<>();
        resultMap.put(new TNetworkAddress("1", 2), new PProcessDictionaryCacheResult());
        resultMap.put(new TNetworkAddress("2", 3), null);

        new Expectations(globalStateMgr) {
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

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new Expectations(systemInfoService) {
            {
                systemInfoService.getBackends();
                minTimes = 0;
                result = backends;

                systemInfoService.getComputeNodes();
                minTimes = 0;
                result = computeNodes;
            }
        };

        new Expectations(dictionaryMgr) {
            {
                dictionaryMgr.getDictionaryStatistic(dictionary);
                minTimes = 0;
                result = new Pair<>(resultMap, "");

                dictionaryMgr.getDictionariesMapById();
                minTimes = 0;
                result = dictionariesMapById;
            }
        };
    }

    @Test
    public void testGetBeOrCn() throws Exception {
        List<TNetworkAddress> nodes = Lists.newArrayList();
        dictionaryMgr.fillBackendsOrComputeNodes(nodes);
    }

    @Test
    public void testShowDictionary() throws Exception {
        dictionaryMgr.getAllInfo("dict");
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

        dictionaryMgr.syncDictionaryMeta(dictionaries);
        dictionaryMgr.scheduleTasks();
        dictionaryMgr.replayModifyDictionaryMgr(dictionaryMgrInfo);
    }

    @Test
    public void testShowDictionaryError() throws Exception {
        DictionaryMgr localDictionaryMgr = new DictionaryMgr();
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key");
        dictionaryValues.add("value");
        Dictionary dictionary =
                    new Dictionary(1, "dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);
        localDictionaryMgr.addDictionary(dictionary);
        new MockUp<DictionaryMgr>() {
            @Mock
            public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                               List<TNetworkAddress> beNodes,
                                                                               List<PProcessDictionaryCacheResult> results) {
                return new Pair<>(true, "test error");
            }
        };

        new MockUp<TimeUtils>() {
            @Mock
            public static synchronized String longToTimeString(long timeStamp) {
                return "0000-01-01 00:00:00";
            }
        };

        List<List<String>> allInfo = localDictionaryMgr.getAllInfo("dict");
        Assertions.assertTrue(
                allInfo.get(0).get(allInfo.get(0).size() - 1).contains("Can not get memory info, errMsg: test error"));
    }

    @Test
    public void testClearDictionaryCache() throws Exception {
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key1");
        dictionaryValues.add("value1");
        Dictionary dictionary =
                new Dictionary(1, "test_dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);
        
        new MockUp<DictionaryMgr>() {
            @Mock
            public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                               List<TNetworkAddress> beNodes,
                                                                               List<PProcessDictionaryCacheResult> results) {
                return new Pair<>(false, "");
            }
        };
        
        DictionaryMgr localDictionaryMgr = new DictionaryMgr();
        localDictionaryMgr.clearDictionaryCache(dictionary, true);
    }

    @Test
    public void testGetDictionaryStatisticSuccess() throws Exception {
        DictionaryMgr localDictionaryMgr = new DictionaryMgr();
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key1");
        dictionaryValues.add("value1");
        Dictionary dictionary =
                new Dictionary(1, "test_dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);

        new MockUp<DictionaryMgr>() {
            @Mock
            public void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                nodes.add(new TNetworkAddress("127.0.0.1", 1234));
            }

            @Mock
            public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                               List<TNetworkAddress> beNodes,
                                                                               List<PProcessDictionaryCacheResult> results) {
                if (results != null) {
                    results.add(new PProcessDictionaryCacheResult());
                }
                return new Pair<>(false, "");
            }
        };

        Pair<Map<TNetworkAddress, PProcessDictionaryCacheResult>, String> result =
                localDictionaryMgr.getDictionaryStatistic(dictionary);
        Assertions.assertNotNull(result.first);
        Assertions.assertEquals("", result.second);
    }

    @Test
    public void testProcessDictionaryCacheException() throws Exception {
        DictionaryMgr localDictionaryMgr = new DictionaryMgr();
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key1");
        dictionaryValues.add("value1");
        Dictionary dictionary =
                new Dictionary(1, "test_dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);

        new MockUp<DictionaryMgr>() {
            @Mock
            public void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                nodes.add(new TNetworkAddress("127.0.0.1", 1234));
            }
        };

        localDictionaryMgr.clearDictionaryCache(dictionary, false);
    }

    @Test
    public void testRefreshDictionaryCacheWorker() throws Exception {
        List<String> dictionaryKeys = Lists.newArrayList();
        List<String> dictionaryValues = Lists.newArrayList();
        dictionaryKeys.add("key1");
        dictionaryValues.add("value1");
        Dictionary dictionary =
                new Dictionary(1, "test_dict", "t", "default_catalog", "testDb", dictionaryKeys, dictionaryValues, null);

        new MockUp<DictionaryMgr>() {
            @Mock
            public void fillBackendsOrComputeNodes(List<TNetworkAddress> nodes) {
                nodes.add(new TNetworkAddress("127.0.0.1", 1234));
            }

            @Mock
            public static Pair<Boolean, String> processDictionaryCacheInteranl(PProcessDictionaryCacheRequest request,
                                                                               List<TNetworkAddress> beNodes,
                                                                               List<PProcessDictionaryCacheResult> results) {
                // Simulate successful begin and commit
                return new Pair<>(false, "");
            }

            @Mock
            public void syncDictionaryMeta(List<Dictionary> dictionaries) {
                // Mock sync operation
            }

            @Mock
            public void unresigerRunningAndUnfinised(long dictionaryId) {
                // Mock unregister operation
            }

            @Mock
            public void updateLastSuccessTxnId(long dictionaryId, long txnId) {
                // Mock update operation
            }
        };

        final DictionaryMgr localDictionaryMgr = new DictionaryMgr();
        DictionaryMgr.RefreshDictionaryCacheWorker worker =
                localDictionaryMgr.new RefreshDictionaryCacheWorker(dictionary, 1L);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public DictionaryMgr getDictionaryMgr() {
                return localDictionaryMgr;
            }
        };
        
        // Test that worker can be instantiated and has proper initialization
        Assertions.assertNotNull(worker);
        worker.run();
    }
}
