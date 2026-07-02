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
import com.starrocks.catalog.LanceTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LanceScanNode extends ScanNode {
    private final LanceTable lanceTable;
    private final List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    public LanceScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        this.lanceTable = (LanceTable) desc.getTable();
    }

    public LanceTable getLanceTable() {
        return lanceTable;
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    @Override
    public boolean isConnectorScanNode() {
        return true;
    }

    public void setupScanRangeLocations() {
        String datasetUri = lanceTable.getDatasetURI();
        Map<String, String> storageOptions = LanceConfig.buildStorageOptions(lanceTable.getProperties());
        ReadOptions.Builder builder = new ReadOptions.Builder();
        if (!storageOptions.isEmpty()) {
            builder.setStorageOptions(storageOptions);
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset dataset = Dataset.open(allocator, datasetUri, builder.build())) {
            for (Fragment fragment : dataset.getFragments()) {
                scanRangeLocationsList.add(toScanRangeLocations(datasetUri, fragment, storageOptions));
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to plan lance scan ranges for %s: %s",
                    datasetUri, e.getMessage());
        }
    }

    private TScanRangeLocations toScanRangeLocations(String datasetUri, Fragment fragment,
                                                     Map<String, String> storageOptions) {
        int rowCount = Math.max(fragment.countRows(), 1);
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setUse_lance_jni_reader(true);
        hdfsScanRange.setDataset_uri(datasetUri);
        hdfsScanRange.setFragment_id(fragment.getId());
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(rowCount);
        hdfsScanRange.setFile_length(rowCount);
        hdfsScanRange.setFile_format(THdfsFileFormat.UNKNOWN);
        hdfsScanRange.setRelative_path(datasetUri + "#" + fragment.getId());
        if (!storageOptions.isEmpty()) {
            hdfsScanRange.setLance_storage_options(storageOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);

        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        scanRangeLocations.setScan_range(scanRange);
        scanRangeLocations.setLocations(new ArrayList<TScanRangeLocation>());
        return scanRangeLocations;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocationsList;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(lanceTable.getName()).append("\n");
        output.append(prefix).append("DATASET URI: ").append(lanceTable.getDatasetURI()).append("\n");
        output.append(prefix).append("PREDICATES: ").append(getExplainString(conjuncts)).append("\n");
        if (columnAccessPaths != null) {
            output.append(explainColumnAccessPath(prefix));
        }
        return output.toString();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        tHdfsScanNode.setTable_name(lanceTable.getName());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        msg.hdfs_scan_node = tHdfsScanNode;
    }
}
