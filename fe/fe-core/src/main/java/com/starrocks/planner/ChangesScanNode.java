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
import com.starrocks.common.Pair;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TChangesScanNode;
import com.starrocks.thrift.TChangesScanRange;
import com.starrocks.thrift.TExplainLevel;
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

/**
 * Plans the per-tablet CHANGES scan for a bookmark-scoped delta. The analyzer
 * guarantees two preconditions before this node is constructed: the
 * BookmarkChange is trackable (every PhysicalPartitionChange is ADDED or
 * DATA_CHANGED) and the OlapTable is bookmark-scoped to head, so every
 * physical partition referenced by the delta still exists with head's
 * index/tablet layout. Violations indicate a planner bug and surface as
 * IllegalStateException.
 */
public class ChangesScanNode extends AbstractOlapTableScanNode {
    private static final Logger LOG = LogManager.getLogger(ChangesScanNode.class);

    private final BookmarkChange delta;
    private final Bookmark base;
    private final Bookmark head;
    // Serialized onto TChangesScanNode.meta_descriptors.
    private final List<ChangesMetaDescriptor> changesMetaDescriptors;
    private final List<TScanRangeLocations> result = new ArrayList<>();

    public ChangesScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable table,
                           BookmarkChange delta, Bookmark base, Bookmark head,
                           List<ChangesMetaDescriptor> changesMetaDescriptors) {
        super(id, desc, "ChangesScanNode", table, table.getBaseIndexMetaId());
        this.delta = delta;
        this.base = base;
        this.head = head;
        this.changesMetaDescriptors = changesMetaDescriptors;
    }

    /**
     * Returns the (baseVersion, headVersion] visible-version pair for one
     * physical-partition change. Throws on non-trackable change types — the
     * analyzer rejects those upstream, so reaching the else is a planner bug.
     */
    private Pair<Long, Long> versionPair(BookmarkChange.PhysicalPartitionChange change) {
        if (change instanceof BookmarkChange.DataChanged dc) {
            return Pair.create(dc.getBasePartition().getVisibleVersion(),
                    dc.getHeadPartition().getVisibleVersion());
        } else if (change instanceof BookmarkChange.PartitionAdded pa) {
            // Partition was absent at base; emit every rowset reachable at head.
            return Pair.create(0L, pa.getHeadPartition().getVisibleVersion());
        } else {
            throw new IllegalStateException(String.format(
                    "non-trackable change in CDC plan for table '%s', physical partition %d: %s",
                    olapTable.getName(), change.getPhysicalPartitionId(), change.getChangeType()));
        }
    }

    public void computeScanRanges(ComputeResource computeResource) {
        long dbId = getSchemaKey().getDb_id();
        long tableId = olapTable.getId();
        for (Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>> entry :
                delta.getChanges().entrySet()) {
            for (BookmarkChange.PhysicalPartitionChange change : entry.getValue()) {
                Pair<Long, Long> versions = versionPair(change);
                long baseVersion = versions.first;
                long headVersion = versions.second;

                long ppId = change.getPhysicalPartitionId();
                PhysicalPartition partition = olapTable.getPhysicalPartition(ppId);
                if (partition == null) {
                    throw new IllegalStateException(
                            "physical partition " + ppId + " missing from bookmark-scoped table '"
                                    + olapTable.getName() + "'");
                }

                MaterializedIndex index = partition.getLatestBaseIndex();
                List<Tablet> tablets = index.getTablets();

                for (Tablet tablet : tablets) {
                    TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

                    TChangesScanRange changesScanRange = new TChangesScanRange();
                    changesScanRange.setDb_id(dbId);
                    changesScanRange.setTable_id(tableId);
                    changesScanRange.setPartition_id(ppId);
                    changesScanRange.setTablet_id(tablet.getId());
                    changesScanRange.setBase_version(baseVersion);
                    changesScanRange.setHead_version(headVersion);

                    TScanRange scanRange = new TScanRange();
                    scanRange.setChanges_scan_range(changesScanRange);
                    scanRangeLocations.setScan_range(scanRange);

                    List<Replica> allQueryableReplicas = Lists.newArrayList();
                    tablet.getQueryableReplicas(allQueryableReplicas, Collections.emptyList(),
                            headVersion, -1, -1, computeResource, null);
                    if (allQueryableReplicas.isEmpty()) {
                        throw new StarRocksPlannerException(
                                "No queryable replica found for CDC scan on tablet " + tablet.getId() +
                                        ". Check if compute nodes are available in the warehouse.",
                                com.starrocks.sql.common.ErrorType.INTERNAL_ERROR);
                    }
                    Collections.shuffle(allQueryableReplicas);
                    boolean hasAliveReplica = false;
                    for (Replica replica : allQueryableReplicas) {
                        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr()
                                .getClusterInfo().getBackendOrComputeNode(replica.getBackendId());
                        if (node == null) {
                            LOG.debug("replica {} not exists", replica.getBackendId());
                            continue;
                        }
                        TScanRangeLocation location = new TScanRangeLocation(
                                new TNetworkAddress(node.getHost(), node.getBePort()));
                        location.setBackend_id(replica.getBackendId());
                        scanRangeLocations.addToLocations(location);
                        hasAliveReplica = true;
                    }
                    if (!hasAliveReplica) {
                        throw new StarRocksPlannerException(
                                "tablet " + tablet.getId() + " have no alive replicas",
                                com.starrocks.sql.common.ErrorType.INTERNAL_ERROR);
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

        int totalTablets = 0;
        for (List<BookmarkChange.PhysicalPartitionChange> changes : delta.getChanges().values()) {
            for (BookmarkChange.PhysicalPartitionChange change : changes) {
                long ppId = change.getPhysicalPartitionId();
                PhysicalPartition pp = olapTable.getPhysicalPartition(ppId);
                if (pp == null) {
                    throw new IllegalStateException(
                            "physical partition " + ppId + " missing from bookmark-scoped table '"
                                    + olapTable.getName() + "'");
                }
                totalTablets += pp.getLatestBaseIndex().getTablets().size();
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
        // BE fetches the live read schema via TableSchemaService keyed by this triple.
        scanNode.setSchema_key(getSchemaKey());
        if (changesMetaDescriptors != null && !changesMetaDescriptors.isEmpty()) {
            scanNode.setMeta_descriptors(changesMetaDescriptors.stream()
                    .map(ChangesMetaDescriptor::toThrift)
                    .toList());
        }

        msg.changes_scan_node = scanNode;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }
}
