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


package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsMetaStateTracker;
import com.starrocks.connector.elasticsearch.EsShardRouting;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.connector.elasticsearch.EsTestCase;
import com.starrocks.connector.elasticsearch.SearchContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class EsScanNodeTest extends EsTestCase {

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    SystemInfoService systemInfoService;

    @BeforeEach
    public void setUp() {
        ComputeNode node1 = new ComputeNode(1, "127.0.0.1", 1000);
        node1.setAlive(true);

        new Expectations() {
            {
                globalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                result = systemInfoService;
                minTimes = 0;

                systemInfoService.backendAndComputeNodeStream();
                result = Stream.of(node1);
            }
        };

    }

    @Test
    public void test()  throws Exception {

        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", IntegerType.BIGINT);
        columns.add(k1);

        Map<String, String> props = new HashMap<>();
        props.put(EsTable.KEY_HOSTS, "http://127.0.0.1:8200");
        props.put(EsTable.KEY_INDEX, "doe");
        props.put(EsTable.KEY_TYPE, "doc");
        props.put(EsTable.KEY_VERSION, "6.5.3");
        props.put(EsTable.KEY_DOC_VALUE_SCAN, "false");
        props.put(EsTable.KEY_KEYWORD_SNIFF, "false");
        EsTable esTable = new EsTable(1L, "doe", columns, props, null);

        TupleDescriptor td = new TupleDescriptor(new TupleId(0));
        td.setTable(esTable);
        PlanNodeId planNodeId = new PlanNodeId(11);
        EsScanNode scanNode = new EsScanNode(planNodeId, td, "EsScanNode", WarehouseManager.DEFAULT_RESOURCE,
                esTable.getMetaSnapshot());
        TPlanNode node = new TPlanNode();
        scanNode.toThrift(node);
        Assertions.assertNotNull(node.getConnector_scan_node());
        Assertions.assertEquals("elasticsearch", node.getConnector_scan_node().getCatalog_type());

        scanNode.assignNodes();

        EsShardPartitions esShardPartitions = EsShardPartitions.findShardPartitions("doe",
                loadJsonFromFile("data/es/test_search_shards.json"));

        List<EsShardPartitions> selectedIndex = new ArrayList<>();
        selectedIndex.add(esShardPartitions);
        scanNode.computeShardLocations(selectedIndex);

        List<EsShardRouting> singleShardRouting = Lists.newArrayList();
        TNetworkAddress addr = new TNetworkAddress("127.0.0.1", 1234);
        singleShardRouting.add(new EsShardRouting("doe", 5, true, addr, "111"));
        esShardPartitions.addShardRouting(5, singleShardRouting);
        scanNode.computeShardLocations(selectedIndex);
    }

    private static void mockSyncFinding(Map<String, String> mappings) {
        new MockUp<EsMetaStateTracker>() {
            @Mock
            public SearchContext run(Invocation invocation) {
                SearchContext context = ((EsMetaStateTracker) invocation.getInvokedInstance()).searchContext();
                context.fetchFieldsContext().putAll(mappings);
                context.docValueFieldsContext().putAll(mappings);
                return context;
            }
        };
        new MockUp<SearchContext>() {
            @Mock
            public EsTablePartitions tablePartitions() {
                return new EsTablePartitions();
            }
        };
    }

    /**
     * The field mappings sent to the BEs are the ones of the sync the query was planned with, not whatever the
     * background sync published since: they used to be read off the table at toThrift time, after the shard
     * routing had already been taken from an earlier sync.
     */
    @Test
    public void testSendsTheMappingsOfThePlannedSnapshot() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(EsTable.KEY_HOSTS, "http://127.0.0.1:8200");
        props.put(EsTable.KEY_INDEX, "doe");
        props.put(EsTable.KEY_TYPE, "doc");
        props.put(EsTable.KEY_VERSION, "6.5.3");
        EsTable esTable = new EsTable(1L, "doe", List.of(new Column("k1", IntegerType.BIGINT)), props, null);
        mockSyncFinding(Map.of("k1", "k1.keyword"));
        esTable.syncTableMetaData(null);
        EsTable.MetaSnapshot planned = esTable.getMetaSnapshot();

        mockSyncFinding(Map.of("k1", "k1.raw"));
        esTable.syncTableMetaData(null);

        TupleDescriptor td = new TupleDescriptor(new TupleId(0));
        td.setTable(esTable);
        EsScanNode scanNode = new EsScanNode(new PlanNodeId(11), td, "EsScanNode", WarehouseManager.DEFAULT_RESOURCE,
                planned);
        scanNode.assignNodes();
        TPlanNode node = new TPlanNode();
        scanNode.toThrift(node);
        Assertions.assertEquals(Map.of("k1", "k1.keyword"), node.getEs_scan_node().getFields_context());
        Assertions.assertEquals(Map.of("k1", "k1.keyword"), node.getEs_scan_node().getDocvalue_context());
    }
}
