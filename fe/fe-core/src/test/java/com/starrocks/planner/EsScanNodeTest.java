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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsShardRouting;
import com.starrocks.connector.elasticsearch.EsTestCase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class EsScanNodeTest extends EsTestCase {

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    SystemInfoService systemInfoService;

    @Before
    public void setUp() {
        ComputeNode node1 = new ComputeNode(1, "127.0.0.1", 1000);
        node1.setAlive(true);

        new Expectations() {
            {
                globalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;

                systemInfoService.backendAndComputeNodeStream();
                result = Stream.of(node1);
            }
        };

    }

    @Test
    public void test(@Mocked Analyzer analyzer,
                     @Mocked EsTable esTable)  throws Exception {

        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", Type.BIGINT);
        columns.add(k1);

        esTable = fakeEsTable("doe", "doe", "doc", columns);

        TupleDescriptor td = new TupleDescriptor(new TupleId(0));
        td.setTable(esTable);
        PlanNodeId planNodeId = new PlanNodeId(11);
        EsScanNode scanNode = new EsScanNode(planNodeId, td, "EsScanNode");

        scanNode.init(analyzer);

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
}
