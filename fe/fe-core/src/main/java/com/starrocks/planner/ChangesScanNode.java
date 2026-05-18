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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TChangesScanNode;
import com.starrocks.thrift.TChangesScanRange;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TKeysType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ChangesScanNode extends AbstractOlapTableScanNode {
    private static final Logger LOG = LogManager.getLogger(ChangesScanNode.class);

    private final BookmarkChange delta;
    private final Bookmark base;
    private final Bookmark head;
    private final List<TScanRangeLocations> result = new ArrayList<>();

    public ChangesScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable table,
                           BookmarkChange delta, Bookmark base, Bookmark head) {
        super(id, desc, "ChangesScanNode", table, table.getBaseIndexMetaId());
        this.delta = delta;
        this.base = base;
        this.head = head;
    }

    /**
     * Extract the base/head visible-version pair for one physical-partition change.
     * Non-trackable changes are rejected upstream by the analyzer/transformer, so we
     * only need to handle ADDED and DATA_CHANGED here.
     */
    private static long[] versionPair(BookmarkChange.PhysicalPartitionChange change) {
        if (change instanceof BookmarkChange.DataChanged) {
            BookmarkChange.DataChanged dc = (BookmarkChange.DataChanged) change;
            return new long[] {
                    dc.getBasePartition().getVisibleVersion(),
                    dc.getHeadPartition().getVisibleVersion()
            };
        } else if (change instanceof BookmarkChange.PartitionAdded) {
            BookmarkChange.PartitionAdded pa = (BookmarkChange.PartitionAdded) change;
            // Partition did not exist at base; emit every rowset reachable at head.
            return new long[] {0L, pa.getHeadPartition().getVisibleVersion()};
        } else {
            throw new IllegalStateException(
                    "non-trackable change in CDC plan: " + change.getChangeType());
        }
    }

    public void computeScanRanges(ComputeResource computeResource) {
        for (Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>> entry :
                delta.getChanges().entrySet()) {
            for (BookmarkChange.PhysicalPartitionChange change : entry.getValue()) {
                BookmarkChange.ChangeType type = change.getChangeType();
                // Skip non-trackable types defensively; analyzer rejects them upstream.
                if (type != BookmarkChange.ChangeType.ADDED
                        && type != BookmarkChange.ChangeType.DATA_CHANGED) {
                    continue;
                }
                long ppId = change.getPhysicalPartitionId();
                PhysicalPartition partition = olapTable.getPhysicalPartition(ppId);
                if (partition == null) {
                    continue;
                }

                long[] versions = versionPair(change);
                long baseVersion = versions[0];
                long headVersion = versions[1];

                // Use headVersion: CDC only needs data up to head, not partition.getVisibleVersion()
                // (which may be higher and would unnecessarily exclude replicas that already satisfy CDC).
                long visibleVersion = headVersion;
                MaterializedIndex index = partition.getLatestBaseIndex();
                List<Tablet> tablets = index.getTablets();

                for (Tablet tablet : tablets) {
                    TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                    TChangesScanRange changesScanRange = new TChangesScanRange();
                    changesScanRange.setTablet_id(tablet.getId());
                    changesScanRange.setBase_version(baseVersion);
                    changesScanRange.setHead_version(headVersion);
                    changesScanRange.setPartition_id(ppId);

                    TInternalScanRange internalRange = new TInternalScanRange();
                    internalRange.setDb_name("");
                    internalRange.setPartition_id(ppId);
                    internalRange.setTablet_id(tablet.getId());
                    internalRange.setVersion(String.valueOf(visibleVersion));
                    internalRange.setVersion_hash("0");
                    internalRange.setSchema_hash(String.valueOf(-1));

                    TScanRange scanRange = new TScanRange();
                    scanRange.setChanges_scan_range(changesScanRange);
                    scanRange.setInternal_scan_range(internalRange);
                    scanRangeLocations.setScan_range(scanRange);

                    List<Replica> allQueryableReplicas = Lists.newArrayList();
                    tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                            visibleVersion, -1, -1, computeResource, null);
                    if (allQueryableReplicas.isEmpty()) {
                        throw new StarRocksPlannerException(
                                "No queryable replica found for CDC scan on tablet " + tablet.getId() +
                                        ". Check if compute nodes are available in the warehouse.",
                                com.starrocks.sql.common.ErrorType.INTERNAL_ERROR);
                    }
                    Collections.shuffle(allQueryableReplicas);
                    for (Replica replica : allQueryableReplicas) {
                        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr()
                                .getClusterInfo().getBackendOrComputeNode(replica.getBackendId());
                        if (node == null) {
                            continue;
                        }
                        TScanRangeLocation location = new TScanRangeLocation(
                                new TNetworkAddress(node.getHost(), node.getBePort()));
                        location.setBackend_id(replica.getBackendId());
                        scanRangeLocations.addToLocations(location);
                        internalRange.addToHosts(new TNetworkAddress(node.getHost(), node.getBePort()));
                    }

                    result.add(scanRangeLocations);
                }
            }
        }
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");

        int totalPartitions = delta.getChanges().size();
        output.append(prefix).append(String.format("partitions=%s/%s\n", totalPartitions, totalPartitions));

        // Count total tablets across trackable changes.
        int totalTablets = 0;
        for (List<BookmarkChange.PhysicalPartitionChange> changes : delta.getChanges().values()) {
            for (BookmarkChange.PhysicalPartitionChange change : changes) {
                BookmarkChange.ChangeType type = change.getChangeType();
                if (type != BookmarkChange.ChangeType.ADDED
                        && type != BookmarkChange.ChangeType.DATA_CHANGED) {
                    continue;
                }
                PhysicalPartition pp = olapTable.getPhysicalPartition(change.getPhysicalPartitionId());
                if (pp != null) {
                    totalTablets += pp.getLatestBaseIndex().getTablets().size();
                }
            }
        }
        int selectedTablets = result.size();
        output.append(prefix).append(String.format("tabletRatio=%s/%s\n", selectedTablets, totalTablets));

        return output.toString();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.CHANGES_SCAN_NODE;

        TChangesScanNode scanNode = new TChangesScanNode();
        scanNode.setTuple_id(desc.getId().asInt());
        scanNode.setTable_type(toThriftKeysType(olapTable.getKeysType()));
        // BE fetches the live read schema via TableSchemaService keyed by this triple.
        scanNode.setSchema_key(getSchemaKey());

        msg.changes_scan_node = scanNode;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }

    private TKeysType toThriftKeysType(KeysType keysType) {
        return switch (keysType) {
            case DUP_KEYS -> TKeysType.DUP_KEYS;
            case AGG_KEYS -> TKeysType.AGG_KEYS;
            case UNIQUE_KEYS -> TKeysType.UNIQUE_KEYS;
            case PRIMARY_KEYS -> TKeysType.PRIMARY_KEYS;
        };
    }
}
