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

import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.paimon.PaimonGlobalIndexMetadataTable;
import com.starrocks.connector.paimon.PaimonGlobalIndexPlanner;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPaimonGlobalIndexScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;

public final class PaimonGlobalIndexScanNode extends MetadataScanNode {
    private final PaimonGlobalIndexMetadataTable table;
    private final HDFSScanNodePredicates predicates = new HDFSScanNodePredicates();
    private final List<TScanRangeLocations> ranges = new ArrayList<>();
    private final CloudConfiguration cloud;
    private String request;

    public PaimonGlobalIndexScanNode(PlanNodeId id, TupleDescriptor descriptor) {
        super(id, descriptor, "PaimonGlobalIndexScanNode");
        table = (PaimonGlobalIndexMetadataTable) descriptor.getTable();
        cloud = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(table.getCatalogName())
                .getMetadata().getCloudConfiguration();
    }

    @Override
    public void preProcessMetadataPredicate(String predicate) {
        request = predicate;
    }

    @Override
    public HDFSScanNodePredicates getScanNodePredicates() {
        return predicates;
    }

    @Override
    public void setupScanRangeLocations() {
        var binding = PaimonGlobalIndexRequest.parseTransport(request);
        PaimonGlobalIndexPlanner.Metadata metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getSerializedMetaSpec(table.getCatalogName(), table.getOriginDb(), table.getOriginTable(),
                        binding.getSnapshotId(), request, MetadataTableType.PAIMON_GLOBAL_INDEX).cast();
        String queryJson = binding.toJson();
        for (var split : metadata.getSplits()) {
            var shard = (PaimonGlobalIndexPlanner.Shard) split;
            TPaimonGlobalIndexScanRange task = new TPaimonGlobalIndexScanRange();
            task.setProtocol_version(PaimonGlobalIndexRequest.CURRENT_VERSION);
            task.setShard_id(shard.id());
            task.setRange_from(shard.from());
            task.setRange_to(shard.to());
            task.setQuery_json(queryJson);
            task.setTable_path(binding.getTablePath());
            task.setSnapshot_id(binding.getSnapshotId());
            task.setIndex_files(shard.files());
            THdfsScanRange hdfs = new THdfsScanRange();
            hdfs.setPaimon_global_index_scan_range(task);
            hdfs.setFull_path(shard.path());
            hdfs.setLength(shard.length());
            hdfs.setFile_length(shard.length());
            hdfs.setOffset(0);
            hdfs.setFile_format(THdfsFileFormat.UNKNOWN);
            TScanRange range = new TScanRange();
            range.setHdfs_scan_range(hdfs);
            TScanRangeLocations locations = new TScanRangeLocations();
            locations.setScan_range(range);
            locations.addToLocations(new TScanRangeLocation(new TNetworkAddress("-1", -1)));
            ranges.add(locations);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long length) {
        return ranges;
    }

    @Override
    protected void toThrift(TPlanNode message) {
        message.setNode_type(TPlanNodeType.HDFS_SCAN_NODE);
        THdfsScanNode node = new THdfsScanNode();
        node.setTuple_id(desc.getId().asInt());
        node.setMetadata_table_type(MetadataTableType.PAIMON_GLOBAL_INDEX.name());
        if (cloud != null) {
            HdfsScanNode.setCloudConfigurationToThrift(node, cloud);
        }
        message.setHdfs_scan_node(node);
    }
}
