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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.PRangeCellPlus.toPRangeCellPlus;

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
    public PartitionDiff diff(Map<String, Range<PartitionKey>> srcRangeMap,
                              Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, PCell> adds = null;
        try {
            Map<String, Range<PartitionKey>> prunedAdd = pruneAddedPartitions(srcRangeMap);
            adds = diffRange(prunedAdd, dstRangeMap);
        } catch (Exception e) {
            LOG.warn("failed to prune partitions when creating");
            throw new RuntimeException(e);
        }
        Map<String, PCell> deletes = diffRange(dstRangeMap, srcRangeMap);
        return new PartitionDiff(adds, deletes);
    }

    /**
     * Prune based on TTL and refresh range
     */
    private Map<String, Range<PartitionKey>> pruneAddedPartitions(Map<String, Range<PartitionKey>> addPartitions)
            throws AnalysisException {
        Map<String, Range<PartitionKey>> res = new HashMap<>(addPartitions);
        if (rangeToInclude != null) {
            res.entrySet().removeIf(entry -> !isRangeIncluded(entry.getValue(), rangeToInclude));
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
            res.values().removeIf(isOutdated);
        }
        if (partitionTTLNumber > 0 && partitionInfo.isRangePartition()) {
            List<PRangeCellPlus> sorted =
                    addPartitions.entrySet()
                            .stream().map(entry -> new PRangeCellPlus(entry.getKey(), entry.getValue()))
                            .sorted(Comparator.reverseOrder())
                            .collect(Collectors.toList());

            Type partitionType = partitionColumns.get(0).getType();

            // For partition ttl, skip partitions that are shadow partitions or in the future
            Predicate<PRangeCellPlus> isShadowKey = Predicates.alwaysFalse();
            Predicate<PRangeCellPlus> isInFuture = Predicates.alwaysFalse();
            // TODO: convert string type to date as predicate
            if (partitionType.isDateType()) {
                PartitionKey shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);
                PartitionKey currentPartitionKey = partitionType.isDatetime() ?
                        PartitionKey.ofDateTime(LocalDateTime.now()) : PartitionKey.ofDate(LocalDate.now());
                isShadowKey = (p) -> p.getCell().getRange().lowerEndpoint().compareTo(shadowPartitionKey) == 0;
                isInFuture = (p) -> p.getCell().getRange().lowerEndpoint().compareTo(currentPartitionKey) > 0;
            }
            // keep partition that either is shadow partition, or larger than current_time
            // and keep only partition_ttl_number of partitions
            Predicate<PRangeCellPlus> finalIsShadowKey = isShadowKey;
            Predicate<PRangeCellPlus> finalIsInFuture = isInFuture;
            List<PRangeCellPlus> ttlCandidate =
                    sorted.stream()
                            .filter(x -> !finalIsShadowKey.test(x) && !finalIsInFuture.test(x))
                            .collect(Collectors.toList());

            // keep only ttl_number of candidates,
            // since ths list already reversed sorted, grab the sublist
            if (ttlCandidate.size() > partitionTTLNumber) {
                ttlCandidate = ttlCandidate.subList(partitionTTLNumber, ttlCandidate.size());
            } else {
                ttlCandidate.clear();
            }

            // remove partitions in ttl candidate
            Set<String> prunedPartitions = ttlCandidate.stream().map(PRangeCellPlus::getPartitionName)
                    .collect(Collectors.toSet());
            res.keySet().removeIf(prunedPartitions::contains);
        }

        return res;
    }

    public static PartitionDiff simpleDiff(Map<String, Range<PartitionKey>> srcRangeMap,
                                           Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, PCell> adds = diffRange(srcRangeMap, dstRangeMap);
        Map<String, PCell> deletes = diffRange(dstRangeMap, srcRangeMap);
        return new PartitionDiff(adds, deletes);
    }

    public static Map<String, PCell> diffRange(Map<String, Range<PartitionKey>> srcRangeMap,
                                               Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, PCell> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeMap.entrySet()) {
            if (!dstRangeMap.containsKey(srcEntry.getKey()) ||
                    !RangeUtils.isRangeEqual(srcEntry.getValue(), dstRangeMap.get(srcEntry.getKey()))) {
                result.put(srcEntry.getKey(),
                        new PRangeCell(SyncPartitionUtils.convertToDatePartitionRange(srcEntry.getValue())));
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
    private static boolean isRangeIncluded(Range<PartitionKey> rangeToCheck, Range<PartitionKey> rangeToInclude) {
        if (rangeToInclude == null) {
            return true;
        }
        int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
        int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
        return !(lowerCmp >= 0 || upperCmp <= 0);
    }

    /**
     * Collect the ref base table's partition range map.
     * @return the ref base table's partition range map: <ref base table, <partition name, partition range>>
     */
    @Override
    public Map<Table, Map<String, PCell>> syncBaseTablePartitionInfos() {
        Map<Table, List<Column>> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        if (partitionTableAndColumn.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<Table, Map<String, PCell>> refBaseTablePartitionMap = Maps.newHashMap();
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
                Map<String, Range<PartitionKey>> refTablePartitionKeyMap =
                        PartitionUtil.getPartitionKeyRange(refBT, refBTPartitionColumns.get(0),
                                mvPartitionExprOpt.get());
                if (refTablePartitionKeyMap == null) {
                    return null;
                }
                Map<String, PCell> refTableRangeCellMap = Maps.newHashMap();
                refTablePartitionKeyMap.entrySet().stream()
                                .forEach(e -> refTableRangeCellMap.put(e.getKey(), new PRangeCell(e.getValue())));
                refBaseTablePartitionMap.put(refBT, refTableRangeCellMap);
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
    private static Map<String, Range<PartitionKey>> mergeRBTPartitionKeyMap(Expr mvPartitionExpr,
                                                                            Map<Table, Map<String, PCell>> basePartitionMap) {
        if (basePartitionMap.size() == 1) {
            Map<String, PCell> first = basePartitionMap.values().iterator().next();
            return PRangeCell.toRangeMap(first);
        }
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (Map<String, PCell> tRangMap : basePartitionMap.values()) {
            for (Map.Entry<String, PCell> add : tRangMap.entrySet()) {
                // TODO: we may implement a new `merge` method in `TreeRangeMap` to merge intersected partitions later.
                Range<PartitionKey> range = ((PRangeCell) add.getValue()).getRange();
                Map<Range<PartitionKey>, String> intersectedRange = addRanges.subRangeMap(range).asMapOfRanges();
                if (intersectedRange.isEmpty()) {
                    addRanges.put(range, add.getKey());
                } else {
                    // To be compatible old version, skip to rename partition name if the intersected partition is the same.
                    if (intersectedRange.size() == 1) {
                        Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                        if (existingRange.equals(range)) {
                            continue;
                        }
                    }
                    addRanges.merge(range, add.getKey(), (o, n) ->
                            String.format("%s_%s_%s", INTERSECTED_PARTITION_PREFIX, o, n));
                }
            }
        }
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<Range<PartitionKey>, String> entry : addRanges.asMapOfRanges().entrySet()) {
            String partitionName = entry.getValue();
            Range<PartitionKey> partitionRange = entry.getKey();
            if (isNeedsRenamePartitionName(partitionName, partitionRange, result)) {
                String mvPartitionName = SyncPartitionUtils.getMVPartitionName(mvPartitionExpr, entry.getKey());
                result.put(mvPartitionName, partitionRange);
            } else {
                result.put(partitionName, partitionRange);
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
                                                      Map<String, Range<PartitionKey>> partitionMap) {
        // if the partition name is intersected with other partitions, rename it.
        if (partitionName.startsWith(INTERSECTED_PARTITION_PREFIX)) {
            return true;
        }
        // if the partition name is already in the partition map, and the partition range is different, rename it.
        if (partitionMap.containsKey(partitionName) && !partitionRange.equals(partitionMap.get(partitionName))) {
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
        Map<Table, Map<String, PCell>> rBTPartitionMap = syncBaseTablePartitionInfos();
        return computePartitionDiff(rangeToInclude, rBTPartitionMap);
    }

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                    Map<Table, Map<String, PCell>> rBTPartitionMap) {
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        Preconditions.checkArgument(!refBaseTablePartitionColumns.isEmpty());
        // get the materialized view's partition range map
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();

        // merge all ref base tables' partition range map to avoid intersected partitions
        Optional<Expr> mvPartitionExprOpt = mv.getRangePartitionFirstExpr();
        if (mvPartitionExprOpt.isEmpty()) {
            return null;
        }
        Expr mvPartitionExpr = mvPartitionExprOpt.get();
        Map<String, Range<PartitionKey>> mergedRBTPartitionKeyMap = mergeRBTPartitionKeyMap(mvPartitionExpr, rBTPartitionMap);
        if (mergedRBTPartitionKeyMap == null) {
            LOG.warn("Merge materialized view {} with base tables failed.", mv.getName());
            return null;
        }
        // only used for checking unaligned partitions in unit test
        if (FeConstants.runningUnitTest) {
            // Check unaligned partitions, unaligned partitions may cause uncorrected result which is not supported for now.
            List<PartitionDiff> rangePartitionDiffList = Lists.newArrayList();
            for (Table refBaseTable : refBaseTablePartitionColumns.keySet()) {
                Preconditions.checkArgument(rBTPartitionMap.containsKey(refBaseTable));
                RangePartitionDiffer differ = new RangePartitionDiffer(mv, isQueryRewrite, rangeToInclude);
                Map<String, PCell> basePartitionToCells = rBTPartitionMap.get(refBaseTable);
                Map<String, Range<PartitionKey>> basePartitionMap = PRangeCell.toRangeMap(basePartitionToCells);
                PartitionDiff diff = PartitionUtil.getPartitionDiff(mvPartitionExpr, basePartitionMap, mvRangePartitionMap,
                        differ);
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
            Map<Table, Map<String, Set<String>>> extRBTMVPartitionNameMap = Maps.newHashMap();
            if (!isQueryRewrite) {
                // To solve multi partition columns' problem of external table, record the mv partition name to all the same
                // partition names map here.
                collectExternalPartitionNameMapping(refBaseTablePartitionColumns, extRBTMVPartitionNameMap);
            }
            return new PartitionDiffResult(extRBTMVPartitionNameMap, rBTPartitionMap,
                    PRangeCell.toCellMap(mvRangePartitionMap), rangePartitionDiff);
        } catch (AnalysisException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
    }

    public static void updatePartitionRefMap(Map<String, Map<Table, Set<String>>> result,
                                             String partitionKey,
                                             Table table, String partitionValue) {
        result.computeIfAbsent(partitionKey, k -> new HashMap<>())
                .computeIfAbsent(table, k -> new HashSet<>())
                .add(partitionValue);
    }

    /**
     * Update the base table partition name to its intersected materialized view names into result.
     * @param result : the final result: BaseTable -> <BaseTablePartitionName -> Set<MVPartitionName>>
     * @param baseTable:: to update the base table
     * @param baseTablePartitionName: base table partition name
     * @param mvPartitionName: materialized view partition name
     */
    public static void updateTableRefMap(Map<Table, Map<String, Set<String>>> result,
                                         Table baseTable,
                                         String baseTablePartitionName,
                                         String mvPartitionName) {
        result.computeIfAbsent(baseTable, k -> new HashMap<>())
                .computeIfAbsent(baseTablePartitionName, k -> new HashSet<>())
                .add(mvPartitionName);
    }

    private static void initialMvRefMap(Map<String, Map<Table, Set<String>>> result,
                                       List<PRangeCellPlus> partitionRanges,
                                       Table table) {
        partitionRanges.stream()
                .forEach(cellPlus -> {
                    result.computeIfAbsent(cellPlus.getPartitionName(), k -> new HashMap<>())
                            .computeIfAbsent(table, k -> new HashSet<>());
                });
    }

    public static void initialBaseRefMap(Map<Table, Map<String, Set<String>>> result,
                                         Table table, PRangeCellPlus partitionRange) {
        result.computeIfAbsent(table, k -> new HashMap<>())
                .computeIfAbsent(partitionRange.getPartitionName(), k -> new HashSet<>());
    }

    /**
     * Generate the reference map between the base table and the mv.
     * @param baseRangeMap src partition list map of the base table
     * @param mvRangeMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    @Override
    public Map<Table, Map<String, Set<String>>> generateBaseRefMap(Map<Table, Map<String, PCell>> baseRangeMap,
                                                                   Map<String, PCell> mvRangeMap) {
        Map<Table, Map<String, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        List<PRangeCellPlus> mvRanges = toPRangeCellPlus(mvRangeMap);
        for (Map.Entry<Table, Map<String, PCell>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PCell> refreshedPartitionsMap = entry.getValue();
            Preconditions.checkState(refBaseTablePartitionExprs.containsKey(baseTable));
            List<Expr> partitionExprs = refBaseTablePartitionExprs.get(baseTable);
            Preconditions.checkArgument(partitionExprs.size() == 1);
            List<PRangeCellPlus> baseRanges = toPRangeCellPlus(refreshedPartitionsMap, partitionExprs.get(0));
            for (PRangeCellPlus baseRange : baseRanges) {
                int mid = Collections.binarySearch(mvRanges, baseRange);
                if (mid < 0) {
                    initialBaseRefMap(result, baseTable, baseRange);
                    continue;
                }
                PRangeCellPlus mvRange = mvRanges.get(mid);
                updateTableRefMap(result, baseTable, baseRange.getPartitionName(), mvRange.getPartitionName());

                int lower = mid - 1;
                while (lower >= 0 && mvRanges.get(lower).isIntersected(baseRange)) {
                    updateTableRefMap(result, baseTable, baseRange.getPartitionName(), mvRanges.get(lower).getPartitionName());
                    lower--;
                }

                int higher = mid + 1;
                while (higher < mvRanges.size() && mvRanges.get(higher).isIntersected(baseRange)) {
                    updateTableRefMap(result, baseTable, baseRange.getPartitionName(), mvRanges.get(higher).getPartitionName());
                    higher++;
                }
            }
        }
        return result;
    }

    /**
     * Generate the mapping from materialized view partition to base table partition.
     * @param mvRangeMap : materialized view partition range map: <partitionName, partitionRange>
     * @param baseRangeMap: base table partition range map, <baseTable, <partitionName, partitionRange>>
     * @return mv partition name -> <base table, base partition names> mapping
     */
    @Override
    public Map<String, Map<Table, Set<String>>> generateMvRefMap(Map<String, PCell> mvRangeMap,
                                                                 Map<Table, Map<String, PCell>> baseRangeMap) {
        Map<String, Map<Table, Set<String>>> result = Maps.newHashMap();
        // for each partition of mv, find all corresponding partition of base
        List<PRangeCellPlus> mvRanges = toPRangeCellPlus(mvRangeMap);

        // [min, max) -> [date_trunc(min), date_trunc(max))
        // if [date_trunc(min), date_trunc(max)), overlap with [mvMin, mvMax), then [min, max) is corresponding partition
        Map<Table, List<PRangeCellPlus>> baseRangesMap = new HashMap<>();
        for (Map.Entry<Table, Map<String, PCell>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PCell> refreshedPartitionsMap = entry.getValue();

            Preconditions.checkState(refBaseTablePartitionExprs.containsKey(baseTable));
            List<Expr> partitionExprs = refBaseTablePartitionExprs.get(baseTable);
            Preconditions.checkArgument(partitionExprs.size() == 1);
            List<PRangeCellPlus> baseRanges = toPRangeCellPlus(refreshedPartitionsMap, partitionExprs.get(0));
            baseRangesMap.put(entry.getKey(), baseRanges);
        }

        for (Map.Entry<Table, List<PRangeCellPlus>> entry : baseRangesMap.entrySet()) {
            initialMvRefMap(result, mvRanges, entry.getKey());
            List<PRangeCellPlus> baseRanges = entry.getValue();
            for (PRangeCellPlus baseRange : baseRanges) {
                int mid = Collections.binarySearch(mvRanges, baseRange);
                if (mid < 0) {
                    continue;
                }
                PRangeCellPlus mvRange = mvRanges.get(mid);
                updatePartitionRefMap(result, mvRange.getPartitionName(), entry.getKey(), baseRange.getPartitionName());

                int lower = mid - 1;
                while (lower >= 0 && mvRanges.get(lower).isIntersected(baseRange)) {
                    updatePartitionRefMap(
                            result, mvRanges.get(lower).getPartitionName(), entry.getKey(), baseRange.getPartitionName());
                    lower--;
                }

                int higher = mid + 1;
                while (higher < mvRanges.size() && mvRanges.get(higher).isIntersected(baseRange)) {
                    updatePartitionRefMap(
                            result, mvRanges.get(higher).getPartitionName(), entry.getKey(), baseRange.getPartitionName());
                    higher++;
                }
            }
        }
        return result;
    }
}