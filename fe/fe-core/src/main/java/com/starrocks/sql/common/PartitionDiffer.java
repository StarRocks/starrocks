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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// TODO: refactor all related code into this class

/**
 * Compute the Materialized View partition mapping from base table
 * e.g. the MV is PARTITION BY date_trunc('month', dt), and the base table is daily partition, the resulted mv
 * should be monthly partition
 */
public class PartitionDiffer {

    private static final Logger LOG = LogManager.getLogger(PartitionDiffer.class);
    private static final String INTERSECTED_PARTITION_PREFIX = "_intersected_";

    private Range<PartitionKey> rangeToInclude;
    private int partitionTTLNumber;
    private PeriodDuration partitionTTL;
    private PartitionInfo partitionInfo;

    public PartitionDiffer(Range<PartitionKey> rangeToInclude, int partitionTTLNumber, PeriodDuration partitionTTL,
                           PartitionInfo partitionInfo) {
        this.rangeToInclude = rangeToInclude;
        this.partitionTTLNumber = partitionTTLNumber;
        this.partitionInfo = partitionInfo;
        this.partitionTTL = partitionTTL;
    }

    public PartitionDiffer() {
    }

    public static PartitionDiffer build(MaterializedView mv,
                                        Pair<String, String> partitionRange)
            throws AnalysisException {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        int partitionTTLNumber = mv.getTableProperty().getPartitionTTLNumber();
        PeriodDuration partitionTTL = mv.getTableProperty().getPartitionTTL();
        Range<PartitionKey> rangeToInclude = null;
        Column partitionColumn =
                ((RangePartitionInfo) partitionInfo).getPartitionColumns().get(0);
        String start = partitionRange.first;
        String end = partitionRange.second;
        if (start != null || end != null) {
            rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        }
        return new PartitionDiffer(rangeToInclude, partitionTTLNumber, partitionTTL, partitionInfo);
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

        if (partitionTTL != null && !partitionTTL.isZero() && partitionInfo instanceof RangePartitionInfo) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns();
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
        if (partitionTTLNumber > 0 && partitionInfo instanceof RangePartitionInfo) {
            List<PartitionRange> sorted =
                    addPartitions.entrySet()
                            .stream().map(entry -> new PartitionRange(entry.getKey(), entry.getValue()))
                            .sorted(Comparator.reverseOrder())
                            .collect(Collectors.toList());

            List<Column> partitionColumns = partitionInfo.getPartitionColumns();
            Type partitionType = partitionColumns.get(0).getType();
            Predicate<PartitionRange> isShadowKey = Predicates.alwaysFalse();
            Predicate<PartitionRange> isInFuture = Predicates.alwaysFalse();
            if (partitionType.isDateType()) {
                PartitionKey currentPartitionKey;
                PartitionKey shadowPartitionKey;
                shadowPartitionKey = PartitionKey.createShadowPartitionKey(partitionColumns);
                currentPartitionKey = partitionType.isDatetime() ?
                        PartitionKey.ofDateTime(LocalDateTime.now()) : PartitionKey.ofDate(LocalDate.now());
                isShadowKey = (p) -> p.getPartitionKeyRange().lowerEndpoint().compareTo(shadowPartitionKey) == 0;
                isInFuture = (p) -> p.getPartitionKeyRange().lowerEndpoint().compareTo(currentPartitionKey) > 0;
            }
            // TODO: convert string type to date as predicate

            // keep partition that either is shadow partition, or larger than current_time
            // and keep only partition_ttl_number of partitions
            Predicate<PartitionRange> finalIsShadowKey = isShadowKey;
            Predicate<PartitionRange> finalIsInFuture = isInFuture;
            List<PartitionRange> ttlCandidate =
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
            Set<String> prunedPartitions =
                    ttlCandidate.stream().map(PartitionRange::getPartitionName)
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
     * @param extBTMVPartitionNameMap the external base table's partition name map which to be updated
     * @return the ref base table's partition range map: <ref base table, <partition name, partition range>>
     */
    private static Map<Table, Map<String, Range<PartitionKey>>> collectRBTPartitionKeyMap(
            Database db,
            MaterializedView mv,
            Expr mvPartitionExpr,
            Map<Table, Map<String, Set<String>>> extBTMVPartitionNameMap) {
        Map<Table, Column> partitionTableAndColumn = mv.getRelatedPartitionTableAndColumn();
        if (partitionTableAndColumn.isEmpty()) {
            return Maps.newHashMap();
        }

        // TODO: lock base tables or use snapshot tables to avoid the partition change during the process.
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
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

                // To solve multi partition columns' problem of external table, record the mv partition name to all the same
                // partition names map here.
                if (!refBT.isNativeTableOrMaterializedView()) {
                    extBTMVPartitionNameMap.put(refBT,
                            PartitionUtil.getMVPartitionNameMapOfExternalTable(refBT,
                                    refBTPartitionColumn, PartitionUtil.getPartitionNames(refBT)));
                }
            }
        } catch (UserException | SemanticException e) {
            LOG.warn("Partition differ collects ref base table partition failed.", e);
            return null;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
                    addRanges.merge(add.getValue(), add.getKey(), (o, n) -> {
                        return String.format("%s_%s_%s", INTERSECTED_PARTITION_PREFIX, o, n);
                    });
                }
            }
        }
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<Range<PartitionKey>, String> entry : addRanges.asMapOfRanges().entrySet()) {
            if (entry.getValue().startsWith(INTERSECTED_PARTITION_PREFIX)) {
                String mvPartitionName = SyncPartitionUtils.getMVPartitionName(mvPartitionExpr, entry.getKey());
                result.put(mvPartitionName, entry.getKey());
            } else {
                result.put(entry.getValue(), entry.getKey());
            }
        }
        return result;
    }

    /**
     * Compute the partition difference between materialized view and all ref base tables.
     * @param mv: the materialized view to check
     * @param partitionRange: <partition start, partition end> pair
     * @return MvPartitionDiffResult: the result of partition difference
     */
    public static MvPartitionDiffResult computePartitionRangeDiff(Database db,
                                                                  MaterializedView mv,
                                                                  Pair<String, String> partitionRange) {
        Expr mvPartitionExpr = mv.getFirstPartitionRefTableExpr();
        Map<Table, Column> refBaseTableAndColumns = mv.getRelatedPartitionTableAndColumn();
        Preconditions.checkArgument(!refBaseTableAndColumns.isEmpty());

        // get the materialized view's partition range map
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();
        // collect all ref base table's partition range map
        Map<Table, Map<String, Set<String>>> extRBTMVPartitionNameMap = Maps.newHashMap();
        Map<Table, Map<String, Range<PartitionKey>>> rBTPartitionMap = collectRBTPartitionKeyMap(db, mv, mvPartitionExpr,
                extRBTMVPartitionNameMap);
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
                    PartitionDiffer differ = PartitionDiffer.build(mv, partitionRange);
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
            PartitionDiffer differ = PartitionDiffer.build(mv, partitionRange);
            RangePartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(mvPartitionExpr, mergedRBTPartitionKeyMap,
                    mvRangePartitionMap, differ);
            if (rangePartitionDiff == null) {
                LOG.warn("Materialized view compute partition difference with base table failed: rangePartitionDiff is null.");
                return null;
            }
            return new MvPartitionDiffResult(mvRangePartitionMap, rBTPartitionMap, extRBTMVPartitionNameMap,
                    rangePartitionDiff);
        } catch (AnalysisException e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
    }
}
