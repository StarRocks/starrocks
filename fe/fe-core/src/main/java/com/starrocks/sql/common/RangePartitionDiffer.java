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

package com.starrocks.sql.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// TODO: refactor all related code into this class
/**
 * Compute the Materialized View partition mapping from base table
 * e.g. the MV is PARTITION BY date_trunc('month', dt), and the base table is daily partition, the resulted mv
 * should be a monthly partition
 */
public final class RangePartitionDiffer extends PartitionDiffer {
    private static final Logger LOG = LogManager.getLogger(RangePartitionDiffer.class);
    private static final String INTERSECTED_PARTITION_PREFIX = "_intersected_";

    private final Range<PartitionKey> rangeToInclude;
    private final int partitionTTLNumber;
    private final PeriodDuration partitionTTL;
    private final PartitionInfo partitionInfo;
    private final List<Column> partitionColumns;
    private final Map<Table, List<Expr>> refBaseTablePartitionExprs;

    public RangePartitionDiffer(MaterializedView mv,
                                boolean isQueryRewrite,
                                Range<PartitionKey> rangeToInclude) {
        super(mv, isQueryRewrite);
        this.partitionInfo = mv.getPartitionInfo();
        this.partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        this.partitionTTL = mv.getTableProperty().getPartitionTTL();
        this.partitionColumns = mv.getPartitionInfo().getPartitionColumns(mv.getIdToColumn());
        this.refBaseTablePartitionExprs = mv.getRefBaseTablePartitionExprs();
        this.rangeToInclude = rangeToInclude;
    }

    /**
     * Diff considering refresh range and TTL
     */
    public PartitionDiff diff(PCellSortedSet srcRangeMap,
                              PCellSortedSet dstRangeMap) {
        Map<String, PCell> adds = null;
        try {
            PCellSortedSet prunedAdd = pruneAddedPartitions(srcRangeMap);
            adds = diffRange(prunedAdd, dstRangeMap);
        } catch (Exception e) {
            LOG.warn("failed to prune partitions when creating");
            throw new RuntimeException(e);
        }
        Map<String, PCell> deletes = diffRange(dstRangeMap, srcRangeMap);
        return new PartitionDiff(PCellSortedSet.of(adds), PCellSortedSet.of(deletes));
    }

    /**
     * Prune based on TTL and refresh range
     */
    private PCellSortedSet pruneAddedPartitions(PCellSortedSet addPartitions)
            throws AnalysisException {
        PCellSortedSet res = PCellSortedSet.of(addPartitions);
        if (rangeToInclude != null) {
            res.removeIf(p -> !isRangeIncluded(((PRangeCell) p.cell()).getRange(), rangeToInclude));
        }

        if (partitionTTL != null && !partitionTTL.isZero() && partitionInfo.isRangePartition()) {
            Type partitionType = partitionColumns.get(0).getType();
            LocalDateTime ttlTime = LocalDateTime.now().minus(partitionTTL);
            PartitionKey ttlLowerBound;
            if (partitionType.isDatetime()) {
                ttlLowerBound = PartitionKey.ofDateTime(ttlTime);
            } else if (partitionType.isDate()) {
                ttlLowerBound = PartitionKey.ofDate(ttlTime.toLocalDate());
            } else {
                throw new SemanticException("partition_ttl not support partition type: " + partitionType);
            }
            Predicate<Range<PartitionKey>> isOutdated = (p) -> p.upperEndpoint().compareTo(ttlLowerBound) <= 0;

            // filter partitions with ttl
            Iterator<PCellWithName> iterator = res.iterator();
            while (iterator.hasNext()) {
                PCellWithName cellWithName = iterator.next();
                PRangeCell rangeCell = (PRangeCell) cellWithName.cell();
                if (isOutdated.test(rangeCell.getRange())) {
                    iterator.remove();
                }
            }
        }
        if (partitionTTLNumber > 0 && partitionInfo.isRangePartition()) {
            Type partitionType = partitionColumns.get(0).getType();

            // For partition ttl, skip partitions that are shadow partitions or in the future
            Predicate<PCellWithName> isShadowKey = Predicates.alwaysFalse();
            Predicate<PCellWithName> isInFuture = Predicates.alwaysFalse();
            // TODO: convert string type to date as predicate
            if (partitionType.isDateType()) {
                PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);
                PartitionKey currentPartitionKey = partitionType.isDatetime() ?
                        PartitionKey.ofDateTime(LocalDateTime.now()) : PartitionKey.ofDate(LocalDate.now());
                isShadowKey = (p) ->
                        ((PRangeCell) p.cell()).getRange().lowerEndpoint().compareTo(shadowPartitionKey) == 0;
                isInFuture = (p) ->
                        ((PRangeCell) p.cell()).getRange().lowerEndpoint().compareTo(currentPartitionKey) > 0;
            }
            // keep partition that either is shadow partition, or larger than current_time
            // and keep only partition_ttl_number of partitions
            Predicate<PCellWithName> finalIsShadowKey = isShadowKey;
            Predicate<PCellWithName> finalIsInFuture = isInFuture;
            List<PCellWithName> ttlCandidate = addPartitions
                    .stream()
                    .filter(x -> !finalIsShadowKey.test(x) && !finalIsInFuture.test(x))
                    .collect(Collectors.toList());
            // sort by range descending
            Collections.reverse(ttlCandidate);

            // keep only ttl_number of candidates,
            // since ths list already reversed sorted, grab the sublist
            if (ttlCandidate.size() > partitionTTLNumber) {
                ttlCandidate = ttlCandidate.subList(partitionTTLNumber, ttlCandidate.size());
            } else {
                ttlCandidate.clear();
            }

            // remove partitions in ttl candidate
            Set<String> prunedPartitions = ttlCandidate
                    .stream()
                    .map(PCellWithName::name)
                    .collect(Collectors.toSet());
            res.removeIf(p -> prunedPartitions.contains(p.name()));
        }

        return res;
    }

    public static PartitionDiff simpleDiff(PCellSortedSet srcRangeMap,
                                           PCellSortedSet dstRangeMap) {
        Map<String, PCell> adds = diffRange(srcRangeMap, dstRangeMap);
        Map<String, PCell> deletes = diffRange(dstRangeMap, srcRangeMap);
        return new PartitionDiff(PCellSortedSet.of(adds), PCellSortedSet.of(deletes));
    }

    public static Map<String, PCell> diffRange(PCellSortedSet srcRangeMap,
                                               PCellSortedSet dstRangeMap) {
        Map<String, PCell> result = Maps.newHashMap();
        for (PCellWithName srcEntry : srcRangeMap.getPartitions()) {
            PRangeCell srcRangeCell = (PRangeCell) srcEntry.cell();
            PCell dstCell = dstRangeMap.getPCell(srcEntry.name());
            if (dstCell == null || !RangeUtils.isRangeEqual(srcRangeCell.getRange(), ((PRangeCell) dstCell).getRange())) {
                result.put(srcEntry.name(),
                        new PRangeCell(SyncPartitionUtils.convertToDatePartitionRange(srcRangeCell.getRange())));
            }
        }
        return result;
    }

    /**
     * Check whether `range` is included in `rangeToInclude`. Here we only want to
     * create partitions which is between `start` and `end` when executing
     * `refresh materialized view xxx partition start (xxx) end (xxx)`
     * @param rangeToInclude range to check whether the to be checked range is in
     * @return true if included, else false
     */
    public static boolean isRangeIncluded(Range<PartitionKey> rangeToCheck, Range<PartitionKey> rangeToInclude) {
        if (rangeToInclude == null) {
            return true;
        }
        int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
        int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
        return !(lowerCmp >= 0 || upperCmp <= 0);
    }

    /**
     * Collect the ref base table's partition range map.
     * @return the ref base table's partition range map: <ref base table, partition cells>
     */
    @Override
    public Map<Table, PCellSortedSet> syncBaseTablePartitionInfos() {
        Map<Table, List<Column>> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        if (partitionTableAndColumn.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<Table, PCellSortedSet> refBaseTablePartitionMap = Maps.newHashMap();
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return Maps.newHashMap();
        }
        try {
            for (Map.Entry<Table, List<Column>> entry : partitionTableAndColumn.entrySet()) {
                Table refBT = entry.getKey();
                List<Column> refBTPartitionColumns = entry.getValue();
                Preconditions.checkArgument(refBTPartitionColumns.size() == 1);
                // Collect the ref base table's partition range map.
                PCellSortedSet refTablePartitionKeyMap =
                        PartitionUtil.getPartitionKeyRange(refBT, refBTPartitionColumns.get(0),
                                mvPartitionExprOpt.get());
                if (refTablePartitionKeyMap == null) {
                    return null;
                }
                refBaseTablePartitionMap.put(refBT, refTablePartitionKeyMap);
            }
        } catch (StarRocksException | SemanticException e) {
            LOG.warn("Partition differ collects ref base table partition failed.", e);
            return null;
        }
        return refBaseTablePartitionMap;
    }

    /**
     * Merge all ref base tables' partition range map to avoid intersected partitions.
     * @param basePartitionMap all ref base tables' partition range map
     * @return merged partition range map: <partition name, partition range>
     */
    private static PCellSortedSet mergeRBTPartitionKeyMap(Expr mvPartitionExpr,
                                                          Map<Table, PCellSortedSet> basePartitionMap) {
        if (basePartitionMap.size() == 1) {
            PCellSortedSet first = basePartitionMap.values().iterator().next();
            return first;
        }
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (PCellSortedSet tRangeCells : basePartitionMap.values()) {
            for (PCellWithName add : tRangeCells.getPartitions()) {
                // TODO: we may implement a new `merge` method in `TreeRangeMap` to merge intersected partitions later.
                Range<PartitionKey> range = ((PRangeCell) add.cell()).getRange();
                Map<Range<PartitionKey>, String> intersectedRange = addRanges.subRangeMap(range).asMapOfRanges();
                if (intersectedRange.isEmpty()) {
                    addRanges.put(range, add.name());
                } else {
                    // To be compatible old version, skip to rename partition name if the intersected partition is the same.
                    if (intersectedRange.size() == 1) {
                        Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                        if (existingRange.equals(range)) {
                            continue;
                        }
                    }
                    addRanges.merge(range, add.name(), (o, n) ->
                            String.format("%s_%s_%s", INTERSECTED_PARTITION_PREFIX, o, n));
                }
            }
        }
        PCellSortedSet result = PCellSortedSet.of();
        for (Map.Entry<Range<PartitionKey>, String> entry : addRanges.asMapOfRanges().entrySet()) {
            String partitionName = entry.getValue();
            Range<PartitionKey> partitionRange = entry.getKey();
            if (isNeedsRenamePartitionName(partitionName, partitionRange, result)) {
                String mvPartitionName = SyncPartitionUtils.getMVPartitionName(mvPartitionExpr, entry.getKey());
                result.add(mvPartitionName, PRangeCell.of(partitionRange));
            } else {
                result.add(partitionName, PRangeCell.of(partitionRange));
            }
        }
        return result;
    }

    /**
     * Check whether the partition name needs to be renamed.
     * @param partitionName: mv temp partition name
     * @param partitionRange: associated partition range
     * @param partitionMap : the partition map to check
     * @return true if needs to rename, else false
     */
    private static boolean isNeedsRenamePartitionName(String partitionName,
                                                      Range<PartitionKey> partitionRange,
                                                      PCellSortedSet partitionMap) {
        // if the partition name is intersected with other partitions, rename it.
        if (partitionName.startsWith(INTERSECTED_PARTITION_PREFIX)) {
            return true;
        }
        // if the partition name is already in the partition map, and the partition range is different, rename it.
        PRangeCell existingCell = (PRangeCell) partitionMap.getPCell(partitionName);
        if (existingCell != null && !existingCell.getRange().equals(partitionRange)) {
            return true;
        }
        return false;
    }

    /**
     * Compute the partition difference between materialized view and all ref base tables.
     * @param rangeToInclude: <partition start, partition end> pair
     * @return MvPartitionDiffResult: the result of partition difference
     */
    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude) {
        Map<Table, PCellSortedSet> rBTPartitionMap = syncBaseTablePartitionInfos();
        return computePartitionDiff(rangeToInclude, rBTPartitionMap);
    }

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                    Map<Table, PCellSortedSet> refBaseTablePartitionMap) {
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        Preconditions.checkArgument(!refBaseTablePartitionColumns.isEmpty());
        // get the materialized view's partition range map
        PCellSortedSet mvRangePartitionMap = mv.getRangePartitionMap();

        // merge all ref base tables' partition range map to avoid intersected partitions
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return null;
        }
        Expr mvPartitionExpr = mvPartitionExprOpt.get();
        PCellSortedSet mergedRBTPartitionKeyMap =
                mergeRBTPartitionKeyMap(mvPartitionExpr, refBaseTablePartitionMap);
        if (mergedRBTPartitionKeyMap == null) {
            LOG.warn("Merge materialized view {} with base tables failed.", mv.getName());
            return null;
        }
        // only used for checking unaligned partitions in unit test
        if (FeConstants.runningUnitTest) {
            // Check unaligned partitions, unaligned partitions may cause uncorrected result which is not supported for now.
            List<PartitionDiff> rangePartitionDiffList = Lists.newArrayList();
            for (Table refBaseTable : refBaseTablePartitionColumns.keySet()) {
                Preconditions.checkArgument(refBaseTablePartitionMap.containsKey(refBaseTable));
                RangePartitionDiffer differ = new RangePartitionDiffer(mv, isQueryRewrite, rangeToInclude);
                PCellSortedSet basePartitionMap = refBaseTablePartitionMap.get(refBaseTable);
                PartitionDiff diff = PartitionUtil.getPartitionDiff(mvPartitionExpr, basePartitionMap,
                        mvRangePartitionMap, differ);
                rangePartitionDiffList.add(diff);
            }
            PartitionDiff.checkRangePartitionAligned(rangePartitionDiffList);
        }
        try {
            // NOTE: Use all refBaseTables' partition range to compute the partition difference between MV and refBaseTables.
            // Merge all deletes of each refBaseTab's diff may cause dropping needed partitions, the deletes should use
            // `bigcap` rather than `bigcup`.
            // Diff_{adds} = P_{\bigcup_{baseTables}^{}} \setminus  P_{MV} \\
            //             = \bigcup_{baseTables} P_{baseTable}\setminus P_{MV}
            //
            // Diff_{deletes} = P_{MV} \setminus P_{\bigcup_{baseTables}^{}} \\
            //                = \bigcap_{baseTables} P_{MV}\setminus P_{baseTable}
            RangePartitionDiffer differ = isQueryRewrite ? null : new RangePartitionDiffer(mv, false, rangeToInclude);
            PartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(mvPartitionExpr, mergedRBTPartitionKeyMap,
                    mvRangePartitionMap, differ);
            if (rangePartitionDiff == null) {
                LOG.warn("Materialized view compute partition difference with base table failed: rangePartitionDiff is null.");
                return null;
            }
            Map<Table, PartitionNameSetMap> extRBTMVPartitionNameMap = Maps.newHashMap();
            if (!isQueryRewrite) {
                // To solve multi partition columns' problem of external table, record the mv partition name to all the same
                // partition names map here.
                collectExternalPartitionNameMapping(refBaseTablePartitionColumns, extRBTMVPartitionNameMap);
            }
            PCellSortedSet mvPartitionCells = mvRangePartitionMap;
            return new PartitionDiffResult(extRBTMVPartitionNameMap, refBaseTablePartitionMap,
                    mvPartitionCells, rangePartitionDiff);
        } catch (AnalysisException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
    }

    public static void updatePartitionRefMap(Map<String, Map<Table, PCellSortedSet>> result,
                                             String partitionKey,
                                             Table table, PCellWithName partitionValue) {
        result.computeIfAbsent(partitionKey, k -> new HashMap<>())
                .computeIfAbsent(table, k -> PCellSortedSet.of())
                .add(partitionValue);
    }

    /**
     * Update the base table partition name to its intersected materialized view names into result.
     * @param result : the final result: BaseTable -> <BaseTablePartitionName -> Set<MVPartitionName>>
     * @param baseTable:: to update the base table
     * @param baseTablePartitionName: base table partition name
     * @param mvPartitionName: materialized view partition name
     */
    public static void updateTableRefMap(Map<Table, PCellSetMapping> result,
                                         Table baseTable,
                                         String baseTablePartitionName,
                                         PCellWithName mvPartitionName) {
        result.computeIfAbsent(baseTable, k -> PCellSetMapping.of())
                .put(baseTablePartitionName, mvPartitionName);
    }

    private static void initialMvRefMap(Map<String, Map<Table, PCellSortedSet>> result,
                                        NavigableSet<PCellWithName> partitionRanges,
                                        Table table) {
        partitionRanges.stream()
                .forEach(cellPlus -> {
                    result.computeIfAbsent(cellPlus.name(), k -> new HashMap<>())
                            .computeIfAbsent(table, k -> PCellSortedSet.of());
                });
    }

    public static void initialBaseRefMap(Map<Table, PCellSetMapping> result,
                                         Table table, PCellWithName partitionRange) {
        result.computeIfAbsent(table, k -> PCellSetMapping.of()).put(partitionRange.name());
    }

    /**
     * Generate the reference map between the base table and the mv.
     * @param basePartitionMaps src partition sorted set of the base table
     * @param mvPartitionMap mv partition sorted set
     * @return base table -> <partition name, mv partition names> mapping
     */
    @Override
    public Map<Table, PCellSetMapping> generateBaseRefMap(Map<Table, PCellSortedSet> basePartitionMaps,
                                                          PCellSortedSet mvPartitionMap) {
        Map<Table, PCellSetMapping> result = Maps.newHashMap();
        if (mvPartitionMap.isEmpty()) {
            for (Map.Entry<Table, PCellSortedSet> entry : basePartitionMaps.entrySet()) {
                Table baseTable = entry.getKey();
                PCellSortedSet refreshedPartitions = entry.getValue();
                for (PCellWithName baseRange : refreshedPartitions.getPartitions()) {
                    initialBaseRefMap(result, baseTable, baseRange);
                }
            }
            return result;
        }

        List<PCellWithName> mvRanges = mvPartitionMap.getPartitions().stream().toList();
        for (Map.Entry<Table, PCellSortedSet> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            PCellSortedSet refreshedPartitions = entry.getValue();
            if (refreshedPartitions.isEmpty()) {
                continue;
            }
            Preconditions.checkState(refBaseTablePartitionExprs.containsKey(baseTable));
            List<Expr> partitionExprs = refBaseTablePartitionExprs.get(baseTable);
            Preconditions.checkArgument(partitionExprs.size() == 1);
            List<PCellWithName> baseRanges = normalizePCellWithNames(refreshedPartitions, partitionExprs.get(0));
            buildBaseToMvIntersectionsMapping(result, baseRanges, mvRanges, baseTable);
        }
        return result;
    }

    /**
     * Optimized intersection finding for base-to-MV mapping using merge-join approach.
     * Time complexity: O(B * M) in worst case with many overlaps, O(B + M) in best case
     *
     * This method finds all MV ranges that intersect with each base range.
     * For each base range, we find all MV ranges that intersect with it.
     */
    private void buildBaseToMvIntersectionsMapping(Map<Table, PCellSetMapping> result,
                                                   List<PCellWithName> baseRanges,
                                                   List<PCellWithName> mvRanges,
                                                   Table baseTable) {
        int mvStartIndex = 0;  // Track where to start searching for each base range

        for (int baseIndex = 0; baseIndex < baseRanges.size(); baseIndex++) {
            PCellWithName baseRange = baseRanges.get(baseIndex);
            Range<PartitionKey> basePartitionRange = ((PRangeCell) baseRange.cell()).getRange();

            boolean foundIntersection = false;
            // For each base range, scan MV ranges starting from mvStartIndex
            int mvIndex = mvStartIndex;
            while (mvIndex < mvRanges.size()) {
                PCellWithName mvRange = mvRanges.get(mvIndex);
                Range<PartitionKey> mvPartitionRange = ((PRangeCell) mvRange.cell()).getRange();
                // If MV range ends before base range starts, skip it and update mvStartIndex
                if (mvPartitionRange.upperEndpoint().compareTo(basePartitionRange.lowerEndpoint()) <= 0) {
                    if (mvIndex == mvStartIndex) {
                        mvStartIndex++;  // This MV range won't intersect with any future base ranges
                    }
                    mvIndex++;
                    continue;
                }

                // If MV range starts after base range ends, no more intersections for this base range
                if (mvPartitionRange.lowerEndpoint().compareTo(basePartitionRange.upperEndpoint()) >= 0) {
                    break;
                }

                // Base Table:
                //  p1: [2023-01-01, 2023-02-01)
                //  p2: [2023-01-01, 2023-02-01)  ← Same range, different name
                //
                //MV:
                //  mv1: [2023-01-01, 2023-02-01)
                //
                //Result: Both p1 and p2 correctly map to mv1
                updateTableRefMap(result, baseTable, baseRange.name(), mvRange);
                foundIntersection = true;
                mvIndex++;
            }

            // If no intersection found, initialize empty mapping
            if (!foundIntersection) {
                initialBaseRefMap(result, baseTable, baseRange);
            }
        }
    }


    /**
     * Generate the mapping from materialized view partition to base table partition.
     * @param mvPCells : materialized view partition sorted set
     * @param baseTablePCells: base table partition sorted set map
     * @return mv partition name -> <base table, base partition names> mapping
     */
    @Override
    public Map<String, Map<Table, PCellSortedSet>> generateMvRefMap(PCellSortedSet mvPCells,
                                                                    Map<Table, PCellSortedSet> baseTablePCells) {
        Map<String, Map<Table, PCellSortedSet>> result = Maps.newHashMap();
        if (mvPCells.isEmpty()) {
            return result;
        }
        // Convert to lists for efficient merge-join operations
        List<PCellWithName> mvRanges = new ArrayList<>(mvPCells.getPartitions());
        for (Map.Entry<Table, PCellSortedSet> entry : baseTablePCells.entrySet()) {
            Table baseTable = entry.getKey();
            PCellSortedSet refreshedPartitions = entry.getValue();
            if (refreshedPartitions.isEmpty()) {
                continue;
            }
            // Initialize empty mappings for this base table
            initialMvRefMap(result, mvPCells.getPartitions(), baseTable);
            Preconditions.checkState(refBaseTablePartitionExprs.containsKey(baseTable));
            List<Expr> partitionExprs = refBaseTablePartitionExprs.get(baseTable);
            Preconditions.checkArgument(partitionExprs.size() == 1);
            // Convert base ranges to list for efficient access
            List<PCellWithName> baseRanges = normalizePCellWithNames(refreshedPartitions, partitionExprs.get(0));
            // Use merge-join optimization for sorted ranges
            buildMVToBaseIntersectionMapping(result, mvRanges, baseRanges, baseTable);
        }
        return result;
    }

    /**
     * Optimized intersection finding using merge-join approach for sorted ranges.
     * Time complexity: O(M * N) in worst case with many overlaps, O(M + N) in best case
     *
     * This method finds all intersections between MV ranges and base ranges.
     * For each MV range, we find all base ranges that intersect with it.
     */
    private void buildMVToBaseIntersectionMapping(Map<String, Map<Table, PCellSortedSet>> result,
                                                  List<PCellWithName> mvRanges,
                                                  List<PCellWithName> baseRanges,
                                                  Table baseTable) {
        int baseStartIndex = 0;  // Track where to start searching for each MV range

        for (int mvIndex = 0; mvIndex < mvRanges.size(); mvIndex++) {
            PCellWithName mvRange = mvRanges.get(mvIndex);
            Range<PartitionKey> mvPartitionRange = ((PRangeCell) mvRange.cell()).getRange();

            // For each MV range, scan base ranges starting from baseStartIndex
            int baseIndex = baseStartIndex;
            while (baseIndex < baseRanges.size()) {
                PCellWithName baseRange = baseRanges.get(baseIndex);
                Range<PartitionKey> basePartitionRange = ((PRangeCell) baseRange.cell()).getRange();

                // If base range ends before MV range starts, skip it and update baseStartIndex
                if (basePartitionRange.upperEndpoint().compareTo(mvPartitionRange.lowerEndpoint()) <= 0) {
                    if (baseIndex == baseStartIndex) {
                        baseStartIndex++;  // This base range won't intersect with any future MV ranges
                    }
                    baseIndex++;
                    continue;
                }

                // If base range starts after MV range ends, no more intersections for this MV range
                if (basePartitionRange.lowerEndpoint().compareTo(mvPartitionRange.upperEndpoint()) >= 0) {
                    break;
                }

                // MV:
                //  mv1: [0, 10)
                //  mv2: [10, 20)
                //  mv3: [20, 30)
                //
                //Base:
                //  b1: [5, 15)   ← Overlaps with mv1 AND mv2
                //  b2: [15, 25)  ← Overlaps with mv2 AND mv3
                //
                //Result: All intersections correctly found:
                //  b1 → {mv1, mv2}
                //  b2 → {mv2, mv3}
                updatePartitionRefMap(result, mvRange.name(), baseTable, baseRange);
                baseIndex++;
            }
        }
    }

    /**
     * Convert a range map to list of partition range cell plus which is sorted by range cell.
     */
    public static List<PCellWithName> normalizePCellWithNames(PCellSortedSet rangeMap,
                                                              Expr expr) {
        if (expr == null || expr instanceof SlotRef) {
            return new ArrayList<>(rangeMap.getPartitions());
        }
        return rangeMap.getPartitions()
                .stream()
                .map(e -> {
                    Range<PartitionKey> partitionKeyRanges = ((PRangeCell) e.cell()).getRange();
                    Range<PartitionKey> convertRanges = SyncPartitionUtils.convertToDatePartitionRange(partitionKeyRanges);
                    return new PCellWithName(e.name(), new PRangeCell(SyncPartitionUtils.transferRange(convertRanges, expr)));
                })
                // this should be already sorted, but just in case
                .sorted(PCellWithName::compareTo)
                .collect(Collectors.toList());
    }
}