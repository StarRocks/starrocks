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
import com.starrocks.common.UserException;
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

    private Range<PartitionKey> rangeToInclude;
    private int partitionTTLNumber;
    private PeriodDuration partitionTTL;
    private PartitionInfo partitionInfo;
    private List<Column> partitionColumns;

    public RangePartitionDiffer(Range<PartitionKey> rangeToInclude,
                           int partitionTTLNumber,
                           PeriodDuration partitionTTL,
                           PartitionInfo partitionInfo,
                           List<Column> partitionColumns) {
        this.rangeToInclude = rangeToInclude;
        this.partitionTTLNumber = partitionTTLNumber;
        this.partitionInfo = partitionInfo;
        this.partitionTTL = partitionTTL;
        this.partitionColumns = partitionColumns;
    }

    public static RangePartitionDiffer build(MaterializedView mv,
                                        Range<PartitionKey> rangeToInclude) throws AnalysisException {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        PeriodDuration partitionTTL = mv.getTableProperty().getPartitionTTL();
        List<Column> partitionColumns = mv.getPartitionInfo().getPartitionColumns(mv.getIdToColumn());
        return new RangePartitionDiffer(rangeToInclude, partitionTTLNumber, partitionTTL, partitionInfo, partitionColumns);
    }

    /**
     * Diff considering refresh range and TTL
     */
    public RangePartitionDiff diff(Map<String, Range<PartitionKey>> srcRangeMap,
                                   Map<String, Range<PartitionKey>> dstRangeMap) {
        RangePartitionDiff res = new RangePartitionDiff();
        try {
            Map<String, Range<PartitionKey>> prunedAdd = pruneAddedPartitions(srcRangeMap);
            res.setAdds(diffRange(prunedAdd, dstRangeMap));
        } catch (Exception e) {
            LOG.warn("failed to prune partitions when creating");
            throw new RuntimeException(e);
        }
        res.setDeletes(diffRange(dstRangeMap, srcRangeMap));
        return res;
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

    public static RangePartitionDiff simpleDiff(Map<String, Range<PartitionKey>> srcRangeMap,
                                                Map<String, Range<PartitionKey>> dstRangeMap) {
        RangePartitionDiff res = new RangePartitionDiff();
        res.setAdds(diffRange(srcRangeMap, dstRangeMap));
        res.setDeletes(diffRange(dstRangeMap, srcRangeMap));
        return res;
    }

    public static Map<String, Range<PartitionKey>> diffRange(Map<String, Range<PartitionKey>> srcRangeMap,
                                                             Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeMap.entrySet()) {
            if (!dstRangeMap.containsKey(srcEntry.getKey()) ||
                    !RangeUtils.isRangeEqual(srcEntry.getValue(), dstRangeMap.get(srcEntry.getKey()))) {
                result.put(srcEntry.getKey(), SyncPartitionUtils.convertToDatePartitionRange(srcEntry.getValue()));
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
     * @param mv the materialized view to compute diff
     * @param basePartitionMaps the external base table's partition name map which to be updated
     * @return the ref base table's partition range map: <ref base table, <partition name, partition range>>
     */
    public static Map<Table, Map<String, Range<PartitionKey>>> syncBaseTablePartitionInfos(MaterializedView mv,
                                                                                           Expr mvPartitionExpr) {
        Map<Table, Column> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        if (partitionTableAndColumn.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<Table, Map<String, Range<PartitionKey>>> refBaseTablePartitionMap = Maps.newHashMap();
        try {
            for (Map.Entry<Table, Column> entry : partitionTableAndColumn.entrySet()) {
                Table refBT = entry.getKey();
                Column refBTPartitionColumn = entry.getValue();
                // Collect the ref base table's partition range map.
                Map<String, Range<PartitionKey>> refTablePartitionKeyMap =
                        PartitionUtil.getPartitionKeyRange(refBT, refBTPartitionColumn,
                                mvPartitionExpr);
                refBaseTablePartitionMap.put(refBT, refTablePartitionKeyMap);
            }
        } catch (UserException | SemanticException e) {
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
    private static Map<String, Range<PartitionKey>> mergeRBTPartitionKeyMap(
            Expr mvPartitionExpr,
            Map<Table, Map<String, Range<PartitionKey>>> basePartitionMap) {

        if (basePartitionMap.size() == 1) {
            return basePartitionMap.values().iterator().next();
        }
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (Map<String, Range<PartitionKey>> tRangMap : basePartitionMap.values()) {
            for (Map.Entry<String, Range<PartitionKey>> add : tRangMap.entrySet()) {
                // TODO: we may implement a new `merge` method in `TreeRangeMap` to merge intersected partitions later.
                Map<Range<PartitionKey>, String> intersectedRange =
                        addRanges.subRangeMap(add.getValue()).asMapOfRanges();
                if (intersectedRange.isEmpty()) {
                    addRanges.put(add.getValue(), add.getKey());
                } else {
                    // To be compatible old version, skip to rename partition name if the intersected partition is the same.
                    if (intersectedRange.size() == 1) {
                        Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                        if (existingRange.equals(add.getValue())) {
                            continue;
                        }
                    }
                    addRanges.merge(add.getValue(), add.getKey(), (o, n) ->
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
     * @param mv: the materialized view to check
     * @param rangeToInclude: <partition start, partition end> pair
     * @param isQueryRewrite: whether it's used for query rewrite or refresh which the difference is that query rewrite will not
     *                      consider partition_ttl_number and mv refresh will consider it to avoid creating too much partitions
     * @return MvPartitionDiffResult: the result of partition difference
     */
    public static RangePartitionDiffResult computeRangePartitionDiff(MaterializedView mv,
                                                                     Range<PartitionKey> rangeToInclude,
                                                                     boolean isQueryRewrite) {
        Expr mvPartitionExpr = mv.getPartitionExpr();
        Map<Table, Map<String, Range<PartitionKey>>> rBTPartitionMap = syncBaseTablePartitionInfos(mv, mvPartitionExpr);
        return computeRangePartitionDiff(mv, rangeToInclude, rBTPartitionMap, isQueryRewrite);
    }

    public static RangePartitionDiffResult computeRangePartitionDiff(
            MaterializedView mv,
            Range<PartitionKey> rangeToInclude,
            Map<Table, Map<String, Range<PartitionKey>>> rBTPartitionMap,
            boolean isQueryRewrite) {
        Expr mvPartitionExpr = mv.getPartitionExpr();
        Map<Table, Column> refBaseTableAndColumns = mv.getRefBaseTablePartitionColumns();
        Preconditions.checkArgument(!refBaseTableAndColumns.isEmpty());
        // get the materialized view's partition range map
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();

        // merge all ref base tables' partition range map to avoid intersected partitions
        Map<String, Range<PartitionKey>> mergedRBTPartitionKeyMap = mergeRBTPartitionKeyMap(mvPartitionExpr, rBTPartitionMap);
        if (mergedRBTPartitionKeyMap == null) {
            LOG.warn("Merge materialized view {} with base tables failed.", mv.getName());
            return null;
        }
        // only used for checking unaligned partitions in unit test
        if (FeConstants.runningUnitTest) {
            try {
                // Check unaligned partitions, unaligned partitions may cause uncorrected result which is not supported for now.
                List<RangePartitionDiff> rangePartitionDiffList = Lists.newArrayList();
                for (Map.Entry<Table, Column> entry : refBaseTableAndColumns.entrySet()) {
                    Table refBaseTable = entry.getKey();
                    RangePartitionDiffer differ = RangePartitionDiffer.build(mv, rangeToInclude);
                    rangePartitionDiffList.add(PartitionUtil.getPartitionDiff(mvPartitionExpr,
                            rBTPartitionMap.get(refBaseTable), mvRangePartitionMap, differ));
                }
                RangePartitionDiff.checkRangePartitionAligned(rangePartitionDiffList);
            } catch (AnalysisException e) {
                LOG.warn("Materialized view compute partition difference with base table failed.", e);
                return null;
            }
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
            RangePartitionDiffer differ = isQueryRewrite ? null : RangePartitionDiffer.build(mv, rangeToInclude);
            RangePartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(mvPartitionExpr, mergedRBTPartitionKeyMap,
                    mvRangePartitionMap, differ);
            if (rangePartitionDiff == null) {
                LOG.warn("Materialized view compute partition difference with base table failed: rangePartitionDiff is null.");
                return null;
            }
            Map<Table, Map<String, Set<String>>> extRBTMVPartitionNameMap = Maps.newHashMap();
            if (!isQueryRewrite) {
                // To solve multi partition columns' problem of external table, record the mv partition name to all the same
                // partition names map here.
                collectExternalPartitionNameMapping(refBaseTableAndColumns, extRBTMVPartitionNameMap);
            }
            return new RangePartitionDiffResult(mvRangePartitionMap, rBTPartitionMap,
                    extRBTMVPartitionNameMap, rangePartitionDiff);
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
     * @param basePartitionExprMap base table to its partitino expr map
     * @param mvRangeMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    public static Map<Table, Map<String, Set<String>>> generateBaseRefMap(
            Map<Table, Map<String, Range<PartitionKey>>> baseRangeMap,
            Map<Table, Expr> basePartitionExprMap,
            Map<String, Range<PartitionKey>> mvRangeMap) {
        Map<Table, Map<String, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        List<PRangeCellPlus> mvRanges = toPRangeCellPlus(mvRangeMap, false);
        for (Map.Entry<Table, Map<String, Range<PartitionKey>>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, Range<PartitionKey>> refreshedPartitionsMap = entry.getValue();
            Preconditions.checkState(basePartitionExprMap.containsKey(baseTable));
            Expr partitionExpr = basePartitionExprMap.get(baseTable);
            List<PRangeCellPlus> baseRanges = toPRangeCellPlus(refreshedPartitionsMap, partitionExpr);
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
     * @param basePartitionExprMap: base table partition expression map, <baseTable, partitionExpr>
     * @param baseRangeMap: base table partition range map, <baseTable, <partitionName, partitionRange>>
     * @return mv partition name -> <base table, base partition names> mapping
     */
    public static Map<String, Map<Table, Set<String>>> generateMvRefMap(
            Map<String, Range<PartitionKey>> mvRangeMap,
            Map<Table, Expr> basePartitionExprMap,
            Map<Table, Map<String, Range<PartitionKey>>> baseRangeMap) {
        Map<String, Map<Table, Set<String>>> result = Maps.newHashMap();
        // for each partition of mv, find all corresponding partition of base
        List<PRangeCellPlus> mvRanges = toPRangeCellPlus(mvRangeMap, false);

        // [min, max) -> [date_trunc(min), date_trunc(max))
        // if [date_trunc(min), date_trunc(max)), overlap with [mvMin, mvMax), then [min, max) is corresponding partition
        Map<Table, List<PRangeCellPlus>> baseRangesMap = new HashMap<>();
        for (Map.Entry<Table, Map<String, Range<PartitionKey>>> entry : baseRangeMap.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, Range<PartitionKey>> refreshedPartitionsMap = entry.getValue();

            Preconditions.checkState(basePartitionExprMap.containsKey(baseTable));
            Expr partitionExpr = basePartitionExprMap.get(baseTable);
            List<PRangeCellPlus> baseRanges = toPRangeCellPlus(refreshedPartitionsMap, partitionExpr);
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