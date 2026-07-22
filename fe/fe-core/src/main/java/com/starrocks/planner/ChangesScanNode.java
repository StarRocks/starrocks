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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.BucketProperty;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    // Logical partition ids surviving partition pruning; null means scan every delta partition.
    private final Set<Long> selectedLogicalPartitionIds;
    // Tablet ids surviving tablet pruning; null means scan every tablet in the selected partitions.
    private final Set<Long> selectedTabletIds;
    private final List<TScanRangeLocations> result = new ArrayList<>();
    // Tablet id -> bucket sequence, numbered over each physical partition's full base-index tablet
    // order (getTabletIdsInOrder(), before the selectedTabletIds filter) and accumulated into one
    // map across every changed partition because tablet ids are globally unique. The bucket sequence
    // keys bucketSeq2locations, the map colocate scheduling uses to co-place matching buckets.
    private final Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
    // a bucket seq may map to many tablets, and each tablet has a TScanRangeLocations.
    private final ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();

    public ChangesScanNode(PlanNodeId id, TupleDescriptor desc, OlapTable table,
                           BookmarkChange delta, Bookmark base, Bookmark head,
                           List<ChangesMetaDescriptor> changesMetaDescriptors,
                           List<Long> selectedLogicalPartitionId, List<Long> selectedTabletId) {
        super(id, desc, "ChangesScanNode", table, table.getBaseIndexMetaId());
        this.delta = delta;
        this.base = base;
        this.head = head;
        this.changesMetaDescriptors = changesMetaDescriptors;
        this.selectedLogicalPartitionIds = selectedLogicalPartitionId == null
                ? null : new HashSet<>(selectedLogicalPartitionId);
        this.selectedTabletIds = selectedTabletId == null
                ? null : new HashSet<>(selectedTabletId);
    }

    public void computeScanRanges(ComputeResource computeResource) {
        long dbId = getSchemaKey().getDb_id();
        long tableId = olapTable.getId();
        DistributionInfo distInfo = olapTable.getDefaultDistributionInfo();
        RangeColocateScanDispatch dispatch = distInfo.getType() == DistributionInfo.DistributionInfoType.RANGE
                ? RangeColocateScanDispatch.forTable(olapTable) : null;
        List<SelectedPhysicalPartition> selectedPartitions = getSelectedPhysicalPartitions(false);
        for (SelectedPhysicalPartition selected : selectedPartitions) {
            PhysicalPartition partition = selected.getPartition();
            BookmarkChange.PhysicalPartitionChange change = selected.getChange();
            long ppId = change.getPhysicalPartitionId();
            Pair<Long, Long> versions = change.versionRange().orElseThrow(() ->
                    new IllegalStateException(String.format(
                            "non-trackable change in CDC plan for table '%s', physical partition %d: %s",
                            olapTable.getName(), ppId, change.getChangeType())));
            long baseVersion = versions.first;
            long headVersion = versions.second;

            MaterializedIndex index = partition.getLatestBaseIndex();
            // Number buckets over the FULL tablet order (before the selectedTabletIds filter) so a
            // tablet's bucket sequence does not depend on which tablets survive pruning; a
            // pruning-dependent numbering would assign the same tablet different bucket seqs across
            // scans and break colocation between co-distributed tables.
            fillTabletId2BucketSeq(dispatch, index, index.getTabletIdsInOrder(), tabletId2BucketSeq);
            List<Tablet> tablets = index.getTablets();

            for (Tablet tablet : tablets) {
                if (selectedTabletIds != null && !selectedTabletIds.contains(tablet.getId())) {
                    continue;
                }
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
                            ErrorType.INTERNAL_ERROR);
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
                            ErrorType.INTERNAL_ERROR);
                }

                bucketSeq2locations.put(tabletId2BucketSeq.get(tablet.getId()), scanRangeLocations);
                result.add(scanRangeLocations);
            }
        }
    }

    // Each physical partition under a selected logical partition, paired with its delta change.
    // When skipEmptyTabletPartitions is set, a partition whose tablets were all pruned away is left out.
    private List<SelectedPhysicalPartition> getSelectedPhysicalPartitions(boolean skipEmptyTabletPartitions) {
        List<SelectedPhysicalPartition> selectedPartitions = new ArrayList<>();
        Set<Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>>> changeEntries = delta.getChanges().entrySet();
        for (Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>> entry : changeEntries) {
            if (selectedLogicalPartitionIds != null && !selectedLogicalPartitionIds.contains(entry.getKey())) {
                continue;
            }
            List<BookmarkChange.PhysicalPartitionChange> partitionChanges = entry.getValue();
            for (BookmarkChange.PhysicalPartitionChange change : partitionChanges) {
                long ppId = change.getPhysicalPartitionId();
                PhysicalPartition partition = olapTable.getPhysicalPartition(ppId);
                if (partition == null) {
                    throw new IllegalStateException(
                            "physical partition " + ppId + " missing from bookmark-scoped table '"
                                    + olapTable.getName() + "'");
                }
                if (skipEmptyTabletPartitions && selectedTabletIds != null
                        && partition.getLatestBaseIndex().getTablets().stream()
                                .noneMatch(t -> selectedTabletIds.contains(t.getId()))) {
                    continue;
                }
                selectedPartitions.add(new SelectedPhysicalPartition(partition, change));
            }
        }
        return selectedPartitions;
    }

    @Override
    public ArrayListMultimap<Integer, TScanRangeLocations> getBucketSeqToLocations() {
        return bucketSeq2locations;
    }

    @Override
    public int getBucketNums() {
        Collection<Long> selectedLogicalPartitions = selectedLogicalPartitionIds != null
                ? selectedLogicalPartitionIds : delta.getChanges().keySet();
        // Skip fully tablet-pruned partitions: the range-colocate alignment check in computeBucketNums
        // must only see partitions that actually contribute scan tablets.
        List<PhysicalPartition> scannedPartitions = getSelectedPhysicalPartitions(true).stream()
                .map(SelectedPhysicalPartition::getPartition).toList();
        return computeBucketNums(olapTable, index.indexMetaId, selectedLogicalPartitions,
                scannedPartitions, tabletId2BucketSeq);
    }

    @Override
    public Optional<List<BucketProperty>> getBucketProperties() throws StarRocksException {
        return Optional.empty();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(olapTable.getName()).append("\n");

        if (!conjuncts.isEmpty()) {
            // VERBOSE uses the lowercase "Predicates:" title with the type-annotated
            // expression, matching how other scan nodes render predicates per level.
            if (detailLevel == TExplainLevel.VERBOSE) {
                output.append(prefix).append("Predicates: ")
                        .append(explainExpr(TExplainLevel.VERBOSE, conjuncts)).append("\n");
            } else {
                output.append(prefix).append("PREDICATES: ").append(explainExpr(conjuncts)).append("\n");
            }
        }

        int totalPartitions = delta.getChanges().size();
        int selectedPartitions = selectedLogicalPartitionIds == null ? totalPartitions
                : (int) delta.getChanges().keySet().stream()
                        .filter(selectedLogicalPartitionIds::contains).count();
        output.append(prefix).append(String.format("partitions=%s/%s\n", selectedPartitions, totalPartitions));

        int totalTablets = 0;
        List<SelectedPhysicalPartition> selectedPhysicalPartitions = getSelectedPhysicalPartitions(false);
        for (SelectedPhysicalPartition selected : selectedPhysicalPartitions) {
            totalTablets += selected.getPartition().getLatestBaseIndex().getTablets().size();
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

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return true;
    }

    // A physical partition surviving logical-partition pruning, paired with its delta change.
    private static class SelectedPhysicalPartition {
        private final PhysicalPartition partition;
        private final BookmarkChange.PhysicalPartitionChange change;

        SelectedPhysicalPartition(PhysicalPartition partition, BookmarkChange.PhysicalPartitionChange change) {
            this.partition = partition;
            this.change = change;
        }

        PhysicalPartition getPartition() {
            return partition;
        }

        BookmarkChange.PhysicalPartitionChange getChange() {
            return change;
        }
    }
}
