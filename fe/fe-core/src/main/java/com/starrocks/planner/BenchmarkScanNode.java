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

import com.google.common.base.MoreObjects;
import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.connector.benchmark.BenchmarkRowCountCalculator;
import com.starrocks.connector.benchmark.RowCountEstimate;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TBenchmarkScanNode;
import com.starrocks.thrift.TBenchmarkScanRange;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.List;

/**
 * Scan node for generated benchmark tables.
 */
public class BenchmarkScanNode extends ScanNode {
    private static final long DEFAULT_ROWS_PER_RANGE = 1_000_000L;

    private final BenchmarkTable table;
    private final List<TScanRangeLocations> scanRangeLocations = new ArrayList<>();
    private long totalRows = Long.MIN_VALUE;

    public BenchmarkScanNode(PlanNodeId id, TupleDescriptor desc, ComputeResource computeResource) {
        super(id, desc, "SCAN BENCHMARK");
        this.table = (BenchmarkTable) desc.getTable();
        this.computeResource = computeResource;
    }

    public void setupScanRangeLocations() {
        if (!scanRangeLocations.isEmpty()) {
            return;
        }
        List<ComputeNode> nodes = LoadScanNode.getAvailableComputeNodes(computeResource);
        if (nodes.isEmpty()) {
            return;
        }

        long resolvedRows = resolveTotalRows();
        if (resolvedRows < 0) {
            TScanRangeLocations locations = new TScanRangeLocations();
            ComputeNode node = nodes.get(0);
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(node.getId());
            location.setServer(new TNetworkAddress(node.getHost(), node.getBePort()));
            locations.addToLocations(location);

            TBenchmarkScanRange benchmarkRange = new TBenchmarkScanRange();
            benchmarkRange.setStart_row(0L);
            benchmarkRange.setRow_count(-1L);

            TScanRange scanRange = new TScanRange();
            scanRange.setBenchmark_scan_range(benchmarkRange);
            locations.setScan_range(scanRange);
            scanRangeLocations.add(locations);
            return;
        }

        long rowsPerRange = DEFAULT_ROWS_PER_RANGE;
        long numRanges = resolvedRows == 0 ? 1 : (resolvedRows + rowsPerRange - 1) / rowsPerRange;

        long start = 0;
        for (long rangeIndex = 0; rangeIndex < numRanges; rangeIndex++) {
            long count = resolvedRows == 0 ? 0 : Math.min(rowsPerRange, resolvedRows - start);
            long rowCount = (rangeIndex == numRanges - 1) ? -1L : count;
            TScanRangeLocations locations = new TScanRangeLocations();
            ComputeNode node = nodes.get((int) (rangeIndex % nodes.size()));
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackend_id(node.getId());
            location.setServer(new TNetworkAddress(node.getHost(), node.getBePort()));
            locations.addToLocations(location);

            TBenchmarkScanRange benchmarkRange = new TBenchmarkScanRange();
            benchmarkRange.setStart_row(start);
            benchmarkRange.setRow_count(rowCount);

            TScanRange scanRange = new TScanRange();
            scanRange.setBenchmark_scan_range(benchmarkRange);
            locations.setScan_range(scanRange);

            scanRangeLocations.add(locations);
            start += count;
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (scanRangeLocations.isEmpty()) {
            setupScanRangeLocations();
        }
        return scanRangeLocations;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.BENCHMARK_SCAN_NODE;
        TBenchmarkScanNode benchmarkScanNode = new TBenchmarkScanNode();
        benchmarkScanNode.setTuple_id(desc.getId().asInt());
        benchmarkScanNode.setTable_name(table.getName());
        benchmarkScanNode.setDb_name(table.getCatalogDBName());
        benchmarkScanNode.setScale_factor(table.getScaleFactor());
        msg.benchmark_scan_node = benchmarkScanNode;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        return helper.addValue(super.debugString()).addValue("table=" + table.getName()).toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(table.getName()).append("\n");
        output.append(prefix).append("DATABASE: ").append(table.getCatalogDBName()).append("\n");
        output.append(prefix).append("SCALE: ").append(table.getScaleFactor()).append("\n");
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    public boolean isRunningAsConnectorOperator() {
        return false;
    }

    private long resolveTotalRows() {
        if (totalRows != Long.MIN_VALUE) {
            return totalRows;
        }
        RowCountEstimate estimate =
                BenchmarkRowCountCalculator.estimateRowCount(table.getCatalogDBName(), table.getName(),
                        table.getScaleFactor());
        totalRows = estimate.isKnown() ? estimate.getRowCount() : -1L;
        return totalRows;
    }
}
