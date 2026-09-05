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
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private Backend availableBackend = new Backend(1, "127.0.0.1", 1234);
    private Backend unavailableBackend = new Backend(3, "127.0.0.3", 1234);
    private ComputeNode availableComputeNode = new ComputeNode(2, "127.0.0.2", 1235);
    private ComputeNode unavailableComputeNode = new ComputeNode(4, "127.0.0.4", 1235);
    private List<Backend> backends = Arrays.asList(availableBackend, unavailableBackend);
    private List<ComputeNode> computeNodes = Arrays.asList(availableComputeNode, unavailableComputeNode);
    private List<Backend> availableBackends = Arrays.asList(availableBackend);
    private List<ComputeNode> availableComputeNodes = Arrays.asList(availableComputeNode);

    private DictionaryMgr dictionaryMgr = new DictionaryMgr();

    @BeforeEach
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

                systemInfoService.getAvailableBackends();
                minTimes = 0;
                result = availableBackends;

                systemInfoService.getAvailableComputeNodes();
                minTimes = 0;
                result = availableComputeNodes;
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

    // Regression: fillBackendsOrComputeNodes must use the available (live) node set. backends/computeNodes
    // carry an extra unavailable node absent from the available set, so reverting to getBackends/
    // getComputeNodes would pull it in here and fail this test.
    @Test
    public void testGetBeOrCn() throws Exception {
        List<TNetworkAddress> nodes = Lists.newArrayList();
        dictionaryMgr.fillBackendsOrComputeNodes(nodes);

        Assertions.assertTrue(nodes.contains(availableBackend.getBrpcAddress()));
        Assertions.assertTrue(nodes.contains(availableComputeNode.getBrpcAddress()));
        Assertions.assertFalse(nodes.contains(unavailableBackend.getBrpcAddress()));
        Assertions.assertFalse(nodes.contains(unavailableComputeNode.getBrpcAddress()));
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
    }

    @Test
    public void testBuildRefreshInterval() throws Exception {
        List<String> dictionaryKeys = Lists.newArrayList("key");
        List<String> dictionaryValues = Lists.newArrayList("value");
        Map<String, String> properties = new HashMap<>();

        // the largest value whose millisecond form still fits an int
        properties.put("dictionary_refresh_interval", "2147483");
        Dictionary dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        dictionary.buildDictionaryProperties();
        Assertions.assertEquals(2147483000, dictionary.getRefreshInterval());

        // zero disables auto refresh
        properties.put("dictionary_refresh_interval", "0");
        dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        dictionary.buildDictionaryProperties();
        Assertions.assertEquals(0, dictionary.getRefreshInterval());

        // negative values are accepted and also disable auto refresh (<= 0 semantics)
        properties.put("dictionary_refresh_interval", "-1");
        dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        dictionary.buildDictionaryProperties();
        Assertions.assertEquals(-1000, dictionary.getRefreshInterval());

        // non-numeric values are rejected
        properties.put("dictionary_refresh_interval", "abc");
        Dictionary nonNumericDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        Assertions.assertThrows(DdlException.class, nonNumericDictionary::buildDictionaryProperties);

        // 30 days in seconds: the millisecond form does not fit an int, so it must be
        // rejected with a clear error instead of wrapping silently
        properties.put("dictionary_refresh_interval", "2592000");
        Dictionary overflowDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        Assertions.assertThrows(DdlException.class, overflowDictionary::buildDictionaryProperties);

        // far beyond the range
        properties.put("dictionary_refresh_interval", "9223372036854775807");
        Dictionary hugeDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        Assertions.assertThrows(DdlException.class, hugeDictionary::buildDictionaryProperties);
    }

    @Test
    public void testSetRefreshingNextSchedulableTime() throws Exception {
        List<String> dictionaryKeys = Lists.newArrayList("key");
        List<String> dictionaryValues = Lists.newArrayList("value");

        // refreshInterval <= 0: a manual refresh or warm up must not reset nextSchedulableTime,
        // otherwise the regular schedule would be re-enabled unexpectedly.
        Dictionary dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, null);
        dictionary.setRefreshing(System.currentTimeMillis());
        Assertions.assertEquals(Long.MAX_VALUE, dictionary.getNextSchedulableTime());

        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_refresh_interval", "-1");
        Dictionary negativeDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        negativeDictionary.buildDictionaryProperties();
        negativeDictionary.setRefreshing(System.currentTimeMillis());
        Assertions.assertEquals(Long.MAX_VALUE, negativeDictionary.getNextSchedulableTime());
        // refreshInterval > 0: next schedulable time is based on the refresh start time
        properties.put("dictionary_refresh_interval", "10"); // 10 seconds
        Dictionary autoRefreshDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        autoRefreshDictionary.buildDictionaryProperties();
        long ts = System.currentTimeMillis();
        autoRefreshDictionary.setRefreshing(ts);
        Assertions.assertEquals(ts + 10000L, autoRefreshDictionary.getNextSchedulableTime());
    }

    @Test
    public void testResetStateHealsStaleNextSchedulableTime() throws Exception {
        List<String> dictionaryKeys = Lists.newArrayList("key");
        List<String> dictionaryValues = Lists.newArrayList("value");

        // Simulate a dictionary loaded from an image written by an older version: auto refresh
        // is disabled (refreshInterval <= 0) but nextSchedulableTime was persisted as a past
        // timestamp, because setRefreshing() used to re-arm the schedule unconditionally.
        // resetState() must heal it, otherwise the regular schedule keeps firing in a loop.
        Dictionary dictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, null);
        dictionary.setNextSchedulableTime(System.currentTimeMillis() - 1000);
        dictionary.resetState();
        Assertions.assertEquals(Long.MAX_VALUE, dictionary.getNextSchedulableTime());

        // refreshInterval > 0: resetState recomputes the next schedule from now
        Map<String, String> properties = new HashMap<>();
        properties.put("dictionary_refresh_interval", "10"); // 10 seconds
        Dictionary autoRefreshDictionary = new Dictionary(1, "dict", "t", "default_catalog", "testDb",
                dictionaryKeys, dictionaryValues, properties);
        autoRefreshDictionary.buildDictionaryProperties();
        long before = System.currentTimeMillis();
        autoRefreshDictionary.resetState();
        long nextSchedulableTime = autoRefreshDictionary.getNextSchedulableTime();
        Assertions.assertTrue(nextSchedulableTime >= before + 10000L);
        Assertions.assertTrue(nextSchedulableTime <= System.currentTimeMillis() + 10000L);
    }
}
