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

import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.connector.benchmark.BenchmarkConfig;
import com.starrocks.connector.benchmark.BenchmarkRowCountCalculator;
import com.starrocks.connector.benchmark.RowCountEstimate;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBenchmarkScanNode;
import com.starrocks.thrift.TBenchmarkScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkScanNodeTest {
    private static final long DEFAULT_ROWS_PER_RANGE = 1_000_000L;
    private static List<ComputeNode> availableNodes;

    @Test
    public void testScanRangeLocationsKnownRows() {
        new MockUp<BenchmarkRowCountCalculator>() {
            @Mock
            public static RowCountEstimate estimateRowCount(String dbName, String tableName, double scaleFactor) {
                return RowCountEstimate.known(2_880_404L);
            }
        };

        List<ComputeNode> nodes = List.of(newComputeNode(1L, "127.0.0.1", 9060),
                newComputeNode(2L, "127.0.0.2", 9061));
        mockAvailableNodes(nodes);

        BenchmarkScanNode scanNode = new BenchmarkScanNode(new PlanNodeId(1),
                newBenchmarkTuple("tpcds", "store_sales", 1.0),
                newComputeResource());

        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(3, locations.size());

        assertRange(locations.get(0), 0L, DEFAULT_ROWS_PER_RANGE, 1L);
        assertRange(locations.get(1), DEFAULT_ROWS_PER_RANGE, DEFAULT_ROWS_PER_RANGE, 2L);
        assertRange(locations.get(2), 2 * DEFAULT_ROWS_PER_RANGE, -1L, 1L);
    }

    @Test
    public void testScanRangeLocationsUnknownRows() {
        new MockUp<BenchmarkRowCountCalculator>() {
            @Mock
            public static RowCountEstimate estimateRowCount(String dbName, String tableName, double scaleFactor) {
                return RowCountEstimate.unknown();
            }
        };

        List<ComputeNode> nodes = List.of(newComputeNode(42L, "127.0.0.3", 9062));
        mockAvailableNodes(nodes);

        BenchmarkScanNode scanNode = new BenchmarkScanNode(new PlanNodeId(1),
                newBenchmarkTuple("tpcds", "store_sales", 1.0),
                newComputeResource());

        List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(1, locations.size());
        assertRange(locations.get(0), 0L, -1L, 42L);
    }

    @Test
    public void testToThrift() {
        BenchmarkScanNode scanNode = new BenchmarkScanNode(new PlanNodeId(1),
                newBenchmarkTuple("tpcds", "store_sales", 1.0),
                newComputeResource());

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);

        Assertions.assertEquals(TPlanNodeType.BENCHMARK_SCAN_NODE, planNode.node_type);
        TBenchmarkScanNode thriftNode = planNode.benchmark_scan_node;
        Assertions.assertNotNull(thriftNode);
        Assertions.assertEquals(0, thriftNode.getTuple_id());
        Assertions.assertEquals("tpcds", thriftNode.getDb_name());
        Assertions.assertEquals("store_sales", thriftNode.getTable_name());
        Assertions.assertEquals(1.0, thriftNode.getScale_factor(), 0.0001);
    }

    private static void assertRange(TScanRangeLocations locations, long startRow, long rowCount, long backendId) {
        Assertions.assertEquals(1, locations.getLocations().size());
        Assertions.assertEquals(backendId, locations.getLocations().get(0).getBackend_id());
        TBenchmarkScanRange range = locations.getScan_range().getBenchmark_scan_range();
        Assertions.assertEquals(startRow, range.getStart_row());
        Assertions.assertEquals(rowCount, range.getRow_count());
    }

    private static void mockAvailableNodes(List<ComputeNode> nodes) {
        availableNodes = nodes;
        new MockUp<LoadScanNode>() {
            @Mock
            public static List<ComputeNode> getAvailableComputeNodes(ComputeResource computeResource) {
                return availableNodes;
            }
        };
    }

    private static TupleDescriptor newBenchmarkTuple(String dbName, String tableName, double scaleFactor) {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        BenchmarkConfig config = new BenchmarkConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, Double.toString(scaleFactor));
        config.loadConfig(properties);
        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        BenchmarkTable table = new BenchmarkTable(1L, "benchmark", dbName, tableName, schema, catalogConfig);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        return desc;
    }

    private static ComputeNode newComputeNode(long id, String host, int bePort) {
        ComputeNode node = new ComputeNode(id, host, 0);
        node.setBePort(bePort);
        return node;
    }

    private static ComputeResource newComputeResource() {
        return new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return 1L;
            }

            @Override
            public long getWorkerGroupId() {
                return 0L;
            }
        };
    }
}
