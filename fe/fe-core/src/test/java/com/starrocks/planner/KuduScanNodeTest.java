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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KuduTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.starrocks.catalog.Type.INT;

public class KuduScanNodeTest {
    @Mocked
    KuduClient client;
    @Mocked
    KuduScanToken token;
    StarRocksAssert starRocksAssert = new StarRocksAssert();
    private final List<KuduScanToken> tokens = new ArrayList<>();
    public static final String KUDU_MASTER = "localhost:7051";
    public static final String KUDU_CATALOG = "kudu_catalog";
    public KuduScanNodeTest() throws IOException {
    }

    @Before
    public void before() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG " + KUDU_CATALOG + " PROPERTIES(" +
                "\"type\"=\"kudu\", " +
                "\"kudu.master\"=\"" + KUDU_MASTER + "\", " +
                "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", " +
                "\"kudu.catalog.type\"=\"hive\")";
        starRocksAssert.withCatalog(createCatalog);
        this.tokens.add(token);
    }

    @After
    public void after() throws Exception {
        starRocksAssert.dropCatalog(KUDU_CATALOG);
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testInit(@Mocked org.apache.kudu.client.KuduTable mockedTable) throws IOException {
        List<Column> columns = createTestColumns();
        KuduTable kuduTable = createTestKuduTable(columns);
        TupleDescriptor tupleDesc = setupDescriptorTable(kuduTable, columns);
        KuduScanNode kuduScanNode = new KuduScanNode(new PlanNodeId(0), tupleDesc, "KuduScanNode");

        byte[] serializedToken = {0, 1, 2, 3};
        List<String> requiredNames = Lists.newArrayList("f0");
        new Expectations() {{
            client.openTable(anyString);
            result = mockedTable;
            client.newScanTokenBuilder((org.apache.kudu.client.KuduTable) any)
                    .setProjectedColumnNames(requiredNames)
                    .build();
            result = tokens;
            token.serialize();
            result = serializedToken;
        }};

        kuduScanNode.setupScanRangeLocations(tupleDesc, null);
        List<TScanRangeLocations> result = kuduScanNode.getScanRangeLocations(1);
        Assert.assertTrue(result.size() > 0);
        TScanRange scanRange = result.get(0).getScan_range();
        Assert.assertTrue(scanRange.isSetHdfs_scan_range());
        THdfsScanRange hdfsScanRange = scanRange.getHdfs_scan_range();
        Assert.assertTrue(hdfsScanRange.getFile_length() > 0);
        Assert.assertTrue(hdfsScanRange.getLength() > 0);
        Assert.assertTrue(hdfsScanRange.isSetUse_kudu_jni_reader());
        Assert.assertEquals("AAECAw", hdfsScanRange.getKudu_scan_token());
        Assert.assertEquals(KUDU_MASTER, hdfsScanRange.getKudu_master());
    }


    private List<Column> createTestColumns() {
        return Lists.newArrayList(new Column("f0", INT), new Column("f1", INT));
    }

    private KuduTable createTestKuduTable(List<Column> columns) {
        return new KuduTable(KUDU_MASTER, KUDU_CATALOG, "db1", "tb1", null, columns, new ArrayList<>());
    }

    private TupleDescriptor setupDescriptorTable(KuduTable kuduTable, List<Column> columns) {
        Analyzer analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), new ConnectContext());
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDesc = descTable.createTupleDescriptor("DestTableTuple");
        tupleDesc.setTable(kuduTable);
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(0), null);
        slotDescriptor.setColumn(columns.get(0));
        tupleDesc.addSlot(slotDescriptor);
        return tupleDesc;
    }

    @Test
    public void testGetAllAvailableBackendOrComputeIdsInSharedNothingMode(@Mocked NodeMgr nodeMgr,
                                                                          @Mocked SystemInfoService systemInfo) {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_NOTHING;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfo;
            }
        };

        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        Backend b2 = new Backend(10002L, "192.168.0.2", 9050);
        ComputeNode c1 = new ComputeNode(20001, "192.168.1.2", 9050);
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Long> getAvailableBackendIds() {
                return new ArrayList<>(List.of(b1.getId(), b2.getId()));
            }

            @Mock
            public List<Long> getAvailableComputeNodeIds() {
                return new ArrayList<>(List.of(c1.getId()));
            }
        };

        KuduScanNode kuduScanNode = makeKuduScanNode();
        List<Long> allAvailableBackendOrComputeIds = kuduScanNode.getAllAvailableBackendOrComputeIds();
        Assert.assertNotNull(allAvailableBackendOrComputeIds);
        Assert.assertEquals(3, allAvailableBackendOrComputeIds.size());
    }

    @Test
    public void testGetAllAvailableBackendOrComputeIdsInSharedDataMode() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        ComputeNode c1 = new ComputeNode(10001L, "192.168.0.2", 9050);
        ComputeNode c2 = new ComputeNode(10002L, "192.168.0.3", 9050);
        mockedWarehouseManager.setAliveComputeNodes(new ArrayList<>(List.of(c1, c2)));

        KuduScanNode kuduScanNode = makeKuduScanNode();
        List<Long> allAvailableBackendOrComputeIds = kuduScanNode.getAllAvailableBackendOrComputeIds();
        Assert.assertNotNull(allAvailableBackendOrComputeIds);
        Assert.assertEquals(2, allAvailableBackendOrComputeIds.size());
    }

    @NotNull
    private KuduScanNode makeKuduScanNode() {
        List<Column> columns = createTestColumns();
        KuduTable kuduTable = createTestKuduTable(columns);
        TupleDescriptor tupleDesc = setupDescriptorTable(kuduTable, columns);
        KuduScanNode kuduScanNode = new KuduScanNode(new PlanNodeId(0), tupleDesc, "KuduScanNode");
        return kuduScanNode;
    }
}