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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.lake.ivm.MvBookmarkOps;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.pct.PCTPartitionTopology;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Decides how much of the materialized view the state-merge join has to read.
 *
 * <p>Derived from the bookmark pair that bounds the delta, never from PCT change metadata: PCT reads base
 * versions live, after the bookmark is taken, so it runs ahead of the delta and would drop a partition
 * whose rows are still to come. Under-reading loses a merge target rather than refreshing less, so
 * anything the bookmarks cannot decide falls back to the whole view.
 */
public final class IvmMvScanPruner {

    private IvmMvScanPruner() {
    }

    /** Empty means read the whole view, which is what every undecidable case returns. */
    public static Set<String> excludedMvPartitions(MaterializedView mv,
                                                   SessionVariable sessionVariable,
                                                   PCTPartitionTopology topology,
                                                   Collection<BaseTableSnapshotInfo> snapshotBaseTables,
                                                   Map<BaseTableInfo, TvrVersionRange> deltas,
                                                   QueryStatement queryStatement) {
        Set<String> toScan =
                mvPartitionsToScan(mv, sessionVariable, topology, snapshotBaseTables, deltas, queryStatement);
        if (toScan == null) {
            return Set.of();
        }
        Set<String> excluded = partitionNameSet();
        mv.getPartitions().stream()
                .map(Partition::getName)
                .filter(name -> !toScan.contains(name))
                .forEach(excluded::add);
        return excluded;
    }

    /** MV partitions the deltas can merge into, or null when any delta cannot be bounded that way. */
    private static Set<String> mvPartitionsToScan(MaterializedView mv,
                                                  SessionVariable sessionVariable,
                                                  PCTPartitionTopology topology,
                                                  Collection<BaseTableSnapshotInfo> snapshotBaseTables,
                                                  Map<BaseTableInfo, TvrVersionRange> deltas,
                                                  QueryStatement queryStatement) {
        if (!sessionVariable.isEnableIvmMvPartitionPruning() || !mv.isPartitionedTable() || topology == null) {
            return null;
        }
        List<TableRelation> queryRelations = AnalyzerUtils.collectTableRelations(queryStatement);
        Map<Long, Long> relationsPerTable = queryRelations.stream()
                .filter(relation -> relation.getTable() != null)
                .collect(Collectors.groupingBy(relation -> relation.getTable().getId(), Collectors.counting()));
        Map<Table, PCellSetMapping> mvPartitionsByBaseTable = topology.getRefBaseTableMVIntersectedPartitions();
        Set<String> result = partitionNameSet();
        boolean sawDelta = false;
        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables) {
            TvrVersionRange delta = deltas.get(snapshotInfo.getBaseTableInfo());
            if (delta == null || delta.isEmpty()) {
                continue;
            }
            sawDelta = true;
            Table baseTable = snapshotInfo.getBaseTable();
            if (!baseTable.isCloudNativeTableOrMaterializedView()) {
                return null;
            }
            // A self-join puts the same table in the partition-reference role and in another one, where its
            // delta joins rows of any partition. The topology is keyed by table, so it cannot tell the two
            // apart and its mapping only describes the reference role.
            if (relationsPerTable.getOrDefault(baseTable.getId(), 1L) > 1) {
                return null;
            }
            Optional<Set<String>> changed = MvBookmarkOps.changedPartitionNames(
                    snapshotInfo.getBaseTableInfo().getDbId(), (OlapTable) baseTable, delta);
            if (changed.isEmpty()) {
                return null;
            }
            // Asked before the topology, which only maps the partition reference: a table that wrote no row
            // joins against nothing, so it needs no partition read on its account even when nothing can map
            // it. Otherwise a compaction on a dimension table alone would send the join over the whole view.
            if (changed.get().isEmpty()) {
                continue;
            }
            PCellSetMapping mvPartitionsByBasePartition = mvPartitionsByBaseTable.get(baseTable);
            if (mvPartitionsByBasePartition == null) {
                return null;
            }
            for (String basePartition : changed.get()) {
                // An empty set is what the differ records for a base partition it could not intersect with any
                // MV partition, so it carries no more information than a missing one.
                PCellSortedSet mvPartitions = mvPartitionsByBasePartition.get(basePartition);
                if (mvPartitions == null || mvPartitions.isEmpty()) {
                    return null;
                }
                result.addAll(mvPartitions.getPartitionNames());
            }
        }
        // A delta whose partitions all turned out to be compaction leaves nothing to merge, so the
        // empty set means read no partition at all -- not that the bookmarks could not decide.
        return sawDelta ? result : null;
    }

    /** Partition names compare case-insensitively across the partition code; these sets have to match that. */
    private static Set<String> partitionNameSet() {
        return new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    }
}
