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

package com.starrocks.planner.lance;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.ipc.Query;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.LanceTable;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TLanceQuery;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LanceScanNode extends ScanNode {

    private final LanceTable lanceTable;
    private Query query;
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.lanceTable = (LanceTable) desc.getTable();
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void computeScanRangeLocations(String datasetURI) {
        BufferAllocator allocator = new RootAllocator(
                RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(4 * 1024 * 1024).build());
        // Storage credentials/endpoint come from the table PROPERTIES, never hardcoded.
        Map<String, String> storageOptions = LanceConfig.buildStorageOptions(lanceTable.getProperties());
        ReadOptions.Builder builder = new ReadOptions.Builder();
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }
        try (Dataset dataset = Dataset.open(allocator, datasetURI, builder.build())) {
            List<Fragment> fragments = dataset.getFragments();
            for (Fragment fragment : fragments) {
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                TScanRange scanRange = new TScanRange();
                THdfsScanRange hdfsScanRange = new THdfsScanRange();
                hdfsScanRange.setFragment_id(fragment.getId());
                hdfsScanRange.setDataset_uri(datasetURI);
                hdfsScanRange.setUse_lance_jni_reader(true);
                if (!storageOptions.isEmpty()) {
                    hdfsScanRange.setLance_storage_options(storageOptions);
                }
                if (query != null) {
                    hdfsScanRange.setLance_query(getQueryThrift());
                }
                // take rows count as length for simple in poc
                hdfsScanRange.setLength(fragment.countRows());
                hdfsScanRange.setFile_length(fragment.countRows());
                scanRange.setHdfs_scan_range(hdfsScanRange);
                scanRangeLocations.setScan_range(scanRange);
                scanRangeLocationsList.add(scanRangeLocations);
            }
        }
    }

    public TLanceQuery getQueryThrift() {
        TLanceQuery lanceQuery = new TLanceQuery();
        lanceQuery.setColumn(query.getColumn());
        lanceQuery.setK(query.getK());
        List<Double> key = new ArrayList<>(query.getKey().length);
        for (float v : query.getKey()) {
            key.add((double) v);
        }
        lanceQuery.setKey(key);
        lanceQuery.setUse_index(query.isUseIndex());
        return lanceQuery;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(lanceTable).append("\n");
        output.append(prefix).append("TUPLE DESC: ").append(desc).append("\n");
        output.append(prefix).append("PREDICATES: ").append(getExplainString(conjuncts)).append("\n");
        output.append(prefix).append("PARTITION COLUMNS: ").append(lanceTable.getPartitionColumns()).append("\n");
        if (query != null) {
            output.append(prefix).append("LANCE QUERY: ").append(query);
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;
        // predicate push down
        if (lanceTable != null) {
            msg.hdfs_scan_node.setTable_name(lanceTable.getName());
        }
    }
}
