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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.NotImplementedException;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public static PartitionDiffer build(MaterializedView materializedView, TaskRunContext context)
            throws AnalysisException {
        Range<PartitionKey> rangeToInclude = null;
        Column partitionColumn =
                ((RangePartitionInfo) materializedView.getPartitionInfo()).getPartitionColumns().get(0);
        String start = context.getProperties().get(TaskRun.PARTITION_START);
        String end = context.getProperties().get(TaskRun.PARTITION_END);
        if (start != null || end != null) {
            rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        }
        int partitionTTLNumber = materializedView.getTableProperty().getPartitionTTLNumber();
        PeriodDuration partitionTTL = materializedView.getTableProperty().getPartitionTTL();
        return new PartitionDiffer(rangeToInclude, partitionTTLNumber, partitionTTL,
                materializedView.getPartitionInfo());
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
            throws NotImplementedException, AnalysisException {
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

    public static Map<String, Range<PartitionKey>> diffRange(List<PartitionRange> srcRanges,
                                                             List<PartitionRange> dstRanges) {
        if (!srcRanges.isEmpty() && !dstRanges.isEmpty()) {
            List<PrimitiveType> srcTypes = srcRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            List<PrimitiveType> dstTypes = dstRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            Preconditions.checkArgument(Objects.equals(srcTypes, dstTypes), "types must be identical");
        }
        List<PartitionRange> diffs = ListUtils.subtract(srcRanges, dstRanges);
        return diffs.stream()
                .collect(Collectors.toMap(PartitionRange::getPartitionName,
                        diff -> SyncPartitionUtils.convertToDatePartitionRange(diff).getPartitionKeyRange()
                ));
    }

    /**
     * Check whether `range` is included in `rangeToInclude`. Here we only want to
     * create partitions which is between `start` and `end` when executing
     * `refresh materialized view xxx partition start (xxx) end (xxx)`
     *
     * @param range          range to check
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

}
