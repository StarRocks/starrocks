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
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TTableSchemaKey;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

// An abstract node to scan data or meta from an OlapTable.
public abstract class AbstractOlapTableScanNode extends ScanNode {

    protected final OlapTable olapTable;
    protected final SelectedMaterializedIndex index;

    /**
     * Constructs a node to scan from an OlapTable.
     * <p>
     * This constructor may access the schema information of the {@link OlapTable}.
     * To ensure thread-safe access, the caller must guarantee that the table's metadata
     * is protected, either by holding an external lock or by operating on a query-specific
     * copy of the table (e.g., created via {@link OlapTable#copyOnlyForQuery(OlapTable)}).
     *
     * @param id The unique identifier for this plan node.
     * @param desc The tuple descriptor for the data produced by this scan node.
     * @param planNodeName The name of the plan node.
     * @param selectedIndexId The ID of the materialized index to be scanned. If this value is -1,
     *                        the base index of the OlapTable will be used.
     */
    public AbstractOlapTableScanNode(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, OlapTable olapTable, long selectedIndexId) {
        super(id, desc, planNodeName);
        this.olapTable = olapTable;
        this.index = SelectedMaterializedIndex.build(olapTable, selectedIndexId);
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public TTableSchemaKey getSchemaKey() {
        TTableSchemaKey schemaKey = new TTableSchemaKey();
        schemaKey.setDb_id(MetaUtils.lookupDbIdByTable(olapTable));
        schemaKey.setTable_id(olapTable.getId());
        schemaKey.setSchema_id(index.schemaId);
        return schemaKey;
    }

    /**
     * Returns the cached schema information for the selected materialized index.
     */
    public Optional<SchemaInfo> getSchema() {
        return index.cachedSchema;
    }

    /**
     * Returns the map from colocation bucket sequence to the scan-range locations of that bucket's
     * tablets, which the colocation-aware backend selector uses to co-place matching buckets. Only a
     * scan that participates in colocation bucket dispatch overrides this; the others do not support it.
     */
    public ArrayListMultimap<Integer, TScanRangeLocations> getBucketSeqToLocations() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not participate in colocation bucket dispatch");
    }

    /**
     * Returns the scan's bucket count for {@code olapTable}. For a HASH table this is the table's
     * bucket count, except that a single selected partition reports its own count, because the
     * partitions of a non-colocate table can have different bucket counts. For a RANGE table it is
     * the colocate group's bucket count when the table is colocated, otherwise the distribution's own
     * count (always 1, one tablet per partition).
     */
    protected static int computeBucketNums(
            OlapTable olapTable, long indexMetaId, Collection<Long> selectedLogicalPartitionIds,
            List<PhysicalPartition> selectedPhysicalPartitions, Map<Long, Integer> tabletId2BucketSeq) {
        DistributionInfo distInfo = olapTable.getDefaultDistributionInfo();
        if (distInfo.getType() == DistributionInfo.DistributionInfoType.RANGE) {
            RangeColocateScanDispatch dispatch = RangeColocateScanDispatch.forTable(olapTable);
            if (dispatch != null) {
                // This bucket-count computation is reached from
                // ExecutionFragment.getOrCreateColocatedAssignment only when BackendSelectorFactory has
                // chosen a colocate-dispatch path. Verify alignment HERE, after the dispatch decision: a
                // misaligned ColocateRangeMgr would silently produce wrong join results under colocate
                // dispatch. Non-colocate scans go through NormalBackendSelector and never reach this point.
                //
                // requireAligned fails closed on two conditions: the group is currently unaligned, OR the
                // bucketSeq this scan actually built (tabletId2BucketSeq) does not match the aligned mapping.
                // The second check closes the fill-vs-dispatch TOCTOU: the bucketSeq fill falls back to a
                // position-based assignment when the group is momentarily unaligned (so a non-colocate scan
                // still works); if the group then re-aligns before this guard runs, comparing the built
                // assignment against the aligned mapping catches the stale position pairing that would
                // otherwise reach the colocate join and silently return wrong results.
                //
                // Invariant: the bucketSeq fill always runs before backend selection reaches this guard, so
                // tabletId2BucketSeq holds the whole scan's built assignment here — an empty map would itself
                // be a not-built/stale state and fails closed.
                dispatch.requireAligned(selectedPhysicalPartitions, indexMetaId, tabletId2BucketSeq);
                return dispatch.bucketCount();
            }
            // Range distribution without a colocate group: RangeDistributionInfo always
            // reports 1 (one tablet per partition by design).
            return distInfo.getBucketNum();
        }
        // HASH path.
        int bucketNum = distInfo.getBucketNum();
        if (selectedLogicalPartitionIds.size() <= 1) {
            for (Long pid : selectedLogicalPartitionIds) {
                bucketNum = olapTable.getPartition(pid).getDistributionInfo().getBucketNum();
            }
        }
        return bucketNum;
    }

    /**
     * Fills {@code target} with the tablet-id → bucket-sequence assignment for {@code selectedIndex},
     * applying the position-fallback policy shared by every scan-time producer of the bucket-sequence map:
     *
     * <ul>
     *   <li>range colocate and aligned — {@code dispatch != null} and
     *       {@link RangeColocateScanDispatch#computeBucketSeq} returns a mapping — uses that mapping;</li>
     *   <li>everything else (HASH, range non-colocate, or a transiently unaligned range colocate group
     *       where {@code computeBucketSeq} returns {@code null}) falls back to position-based bucketSeq.</li>
     * </ul>
     *
     * <p>Entries are added to {@code target} without clearing it, so a caller can accumulate the
     * whole-scan assignment across sub-partitions. The dispatch-time alignment guard in
     * {@link #computeBucketNums} fails closed on the colocate-dispatch path when the built assignment is a
     * stale position fallback, so no unaligned observation needs to be recorded here.
     */
    public static void fillTabletId2BucketSeq(@Nullable RangeColocateScanDispatch dispatch,
                                              MaterializedIndex selectedIndex,
                                              List<Long> allTabletIds,
                                              Map<Long, Integer> target) {
        if (dispatch != null) {
            Map<Long, Integer> rangeColocateMap = dispatch.computeBucketSeq(selectedIndex);
            if (rangeColocateMap != null) {
                target.putAll(rangeColocateMap);
                return;
            }
        }
        for (int i = 0; i < allTabletIds.size(); i++) {
            target.put(allTabletIds.get(i), i);
        }
    }


    /** Selected materialized index to scan. */
    protected static class SelectedMaterializedIndex {

        final long indexMetaId;
        final long schemaId;

        /**
         * Cached schema information used for query plan.
         * <p>
         * This field stores the schema information that was used when generating the plan, enabling
         * backend nodes to retrieve the exact schema from the frontend during query execution. This is
         * particularly important for Fast Schema Evolution scenarios where schema changes are not
         * immediately propagated schema metadata to backend nodes. Currently, this mechanism is only
         * used in shared-data mode.
         * </p>
         */
        private final Optional<SchemaInfo> cachedSchema;

        private SelectedMaterializedIndex(long indexMetaId, long schemaId, @Nullable SchemaInfo cachedSchema) {
            this.indexMetaId = indexMetaId;
            this.schemaId = schemaId;
            this.cachedSchema = Optional.ofNullable(cachedSchema);
        }

        static SelectedMaterializedIndex build(OlapTable olapTable, long selectedIndexMetaId) {
            long indexMetaId = selectedIndexMetaId == -1 ? olapTable.getBaseIndexMetaId() : selectedIndexMetaId;
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByMetaId(indexMetaId);
            if (indexMeta == null) {
                throw new RuntimeException(String.format(
                        "can't find index, table name: %s, table id: %s, index meta id: %s",
                        olapTable.getName(), olapTable.getId(), indexMetaId));
            }
            long schemaId = indexMeta.getSchemaId();
            SchemaInfo schema = null;
            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                schema = SchemaInfo.fromMaterializedIndex(olapTable, indexMetaId, indexMeta);
            }
            return new SelectedMaterializedIndex(indexMetaId, schemaId, schema);
        }
    }
}
