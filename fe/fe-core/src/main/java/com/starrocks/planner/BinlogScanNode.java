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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.exception.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.StreamSourceType;
import com.starrocks.thrift.TBinlogOffset;
import com.starrocks.thrift.TBinlogScanNode;
import com.starrocks.thrift.TBinlogScanRange;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStreamScanNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * BinlogScanNode read data from binlog of SR table
 */
public class BinlogScanNode extends ScanNode {

    private static final Logger LOG = LogManager.getLogger(BinlogScanNode.class);

    private boolean isFinalized = false;
    private OlapTable olapTable;
    private List<Long> tabletIds;
    private Set<Long> scanBackendIds;
    private List<TScanRangeLocations> scanRanges;

    public BinlogScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc, "BinlogScanNode");
        this.tabletIds = new ArrayList<>();
        this.scanBackendIds = new HashSet<>();
        this.scanRanges = new ArrayList<>();
        olapTable = (OlapTable) Preconditions.checkNotNull(desc.getTable());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        TBinlogScanNode binlogScan = new TBinlogScanNode();
        binlogScan.setTuple_id(desc.getId().asInt());

        TStreamScanNode streamScan = new TStreamScanNode();
        streamScan.setBinlog_scan(binlogScan);
        streamScan.setSource_type(StreamSourceType.BINLOG);

        msg.setStream_scan_node(streamScan);
        msg.setNode_type(TPlanNodeType.STREAM_SCAN_NODE);
    }

    protected TBinlogOffset getBinlogOffset(long tabletId) {
        TBinlogOffset offset = new TBinlogOffset();
        offset.setTablet_id(tabletId);
        // TODO -1 indicates that read from the oldest binlog
        offset.setVersion(-1);
        offset.setLsn(-1);
        return offset;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
    }

    @Override
    public void computeStats(Analyzer analyzer) {
    }

    @Override
    public void finalizeStats(Analyzer analyzer) throws UserException {
        if (isFinalized) {
            return;
        }
        computeScanRanges();
        computeStats(analyzer);
        isFinalized = true;
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("table: ").append(olapTable.getName());
        sb.append(prefix).append(String.format("tabletList: %s", Joiner.on(",").join(tabletIds)));
        return sb.toString();
    }

    // TODO: support partition prune and bucket prune
    public void computeScanRanges() throws UserException {
        scanRanges = new ArrayList<>();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        long localBeId = -1;
        long dbId = -1;
        String dbName = null;
        for (PhysicalPartition partition : CollectionUtils.emptyIfNull(olapTable.getAllPhysicalPartitions())) {
            MaterializedIndex table = partition.getBaseIndex();
            long partitionId = partition.getId();
            long tableId = olapTable.getId();

            for (Tablet tablet : CollectionUtils.emptyIfNull(table.getTablets())) {
                if (dbId == -1) {
                    TabletMeta meta = invertedIndex.getTabletMeta(tablet.getId());
                    dbId = meta.getDbId();
                    dbName = GlobalStateMgr.getCurrentState().getDb(dbId).getFullName();
                }

                long tabletId = tablet.getId();
                tabletIds.add(tabletId);
                TBinlogOffset binlogOffset = getBinlogOffset(tabletId);
                TBinlogScanRange binlogRange = new TBinlogScanRange();
                binlogRange.setTablet_id(tabletId);
                binlogRange.setOffset(binlogOffset);
                binlogRange.setPartition_id(partitionId);
                binlogRange.setTable_id(tableId);
                binlogRange.setDb_name(dbName);

                TScanRange scanRange = new TScanRange();
                scanRange.setBinlog_scan_range(binlogRange);
                TScanRangeLocations locations = new TScanRangeLocations();
                locations.setScan_range(scanRange);

                // Choose replicas
                int schemaHash = olapTable.getSchemaHashByIndexId(olapTable.getBaseIndexId());
                long visibleVersion = partition.getVisibleVersion();

                List<Replica> allQueryableReplicas = Lists.newArrayList();
                List<Replica> localReplicas = Lists.newArrayList();
                tablet.getQueryableReplicas(allQueryableReplicas, localReplicas, visibleVersion, localBeId, schemaHash);
                if (CollectionUtils.isEmpty(allQueryableReplicas)) {
                    throw new UserException("No queryable replica for tablet " + tabletId);
                }
                for (Replica replica : allQueryableReplicas) {
                    Backend backend = Preconditions.checkNotNull(
                            GlobalStateMgr.getCurrentSystemInfo().getBackend(replica.getBackendId()),
                            "backend not found: " + replica.getBackendId());
                    scanBackendIds.add(backend.getId());
                    TScanRangeLocation replicaLocation = new TScanRangeLocation(backend.getAddress());
                    replicaLocation.setBackend_id(backend.getId());
                    locations.addToLocations(replicaLocation);
                }

                scanRanges.add(locations);
            }

        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRanges;
    }

    @Override
    public int getNumInstances() {
        return scanRanges.size();
    }

    @Override
    public boolean canDoReplicatedJoin() {
        return false;
    }
}
