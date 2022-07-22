// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;


import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Process lower bound and upper bound for Expression Partition,
 * only support SlotRef and FunctionCallExpr
 */
public class ExpressionPartitionUtil {
    private static final String DEFAULT_PREFIX = "p";

    public static PartitionDiff calcSyncSamePartition(Map<String, Range<PartitionKey>> baseRangeMap,
                                                      Map<String, Range<PartitionKey>> mvRangeMap) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        Map<String, Range<PartitionKey>> adds = diffRange(baseRangeMap, mvRangeMap);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, baseRangeMap);
        return new PartitionDiff(adds, deletes);
    }

    public static PartitionDiff calcSyncRollupPartition(Map<String, Range<PartitionKey>> baseRangeMap,
                                                        Map<String, Range<PartitionKey>> mvRangeMap,
                                                        String granularity) {
        Map<String, Range<PartitionKey>> rollupRange = mappingRangeList(baseRangeMap, granularity);
        Map<String, Range<PartitionKey>> adds = diffRange(rollupRange, mvRangeMap);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, rollupRange);
        return new PartitionDiff(adds, deletes);

    }

    public static Map<String, Range<PartitionKey>> mappingRangeList(Map<String, Range<PartitionKey>> baseRangeMap,
                                                              String granularity) {
        Set<LocalDateTime> timePointSet = Sets.newTreeSet();
        for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
            PartitionMapping mappedRange = mappingRange(rangeEntry.getValue(), granularity);
            // this mappedRange may exist range overlap
            timePointSet.add(mappedRange.getLowerDateTime());
            timePointSet.add(mappedRange.getUpperDateTime());
        }
        List<LocalDateTime> timePointList = Lists.newArrayList(timePointSet);
        // deal overlap
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        if (timePointList.size() < 2) {
            return result;
        }
        for (int i = 1; i < timePointList.size(); i++) {
            try {
                PartitionKey lowerPartitionKey = new PartitionKey();
                LocalDateTime lowerDateTime = timePointList.get(i - 1);
                lowerPartitionKey.pushColumn(new DateLiteral(lowerDateTime,
                        Type.DATETIME), PrimitiveType.DATETIME);
                LocalDateTime upperDateTime = timePointList.get(i);
                PartitionKey upperPartitionKey = new PartitionKey();
                upperPartitionKey.pushColumn(new DateLiteral(upperDateTime,
                        Type.DATETIME), PrimitiveType.DATETIME);
                String mvPartitionName = getMVPartitionName(lowerDateTime, upperDateTime, granularity);
                result.put(mvPartitionName, Range.closedOpen(lowerPartitionKey, upperPartitionKey));
            } catch (AnalysisException ex) {
                throw new SemanticException("Convert to DateLiteral failed:", ex);
            }
        }
        return result;
    }

    public static PartitionMapping mappingRange(Range<PartitionKey> baseRange, String granularity) {
        // assume expr partition must be DateLiteral and only one partition
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        Preconditions.checkArgument(lowerExpr instanceof DateLiteral);
        Preconditions.checkArgument(upperExpr instanceof DateLiteral);
        DateLiteral lowerDate = (DateLiteral) lowerExpr;
        DateLiteral upperDate = (DateLiteral) upperExpr;
        LocalDateTime lowerDateTime = lowerDate.toLocalDateTime();
        LocalDateTime upperDateTime = upperDate.toLocalDateTime();

        LocalDateTime truncLowerDateTime = getLowerDateTime(lowerDateTime, granularity);
        LocalDateTime truncUpperDateTime = getUpperDateTime(upperDateTime, granularity);
        return new PartitionMapping(truncLowerDateTime, truncUpperDateTime);
    }

    public static String getMVPartitionName(LocalDateTime lowerDateTime, LocalDateTime upperDateTime,
                                            String granularity) {
        switch (granularity) {
            case "minute":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.MINUTE_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.MINUTE_FORMATTER);
            case "hour":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.HOUR_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.HOUR_FORMATTER);
            case "day":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.DATEKEY_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.DATEKEY_FORMATTER);
            case "month":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.MONTH_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.MONTH_FORMATTER);
            case "quarter":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.QUARTER_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.QUARTER_FORMATTER);
            case "year":
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.YEAR_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.YEAR_FORMATTER);
            default:
                throw new SemanticException("Do not support date_trunc format string:{}", granularity);
        }
    }

    // when the upperDateTime is the same as granularity rollup time, should not +1
    @NotNull
    private static LocalDateTime getUpperDateTime(LocalDateTime upperDateTime, String granularity) {
        LocalDateTime truncUpperDateTime;
        switch (granularity) {
            case "minute":
                if (upperDateTime.withNano(0).withSecond(0).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusMinutes(1).withNano(0).withSecond(0);
                }
                break;
            case "hour":
                if (upperDateTime.withNano(0).withSecond(0).withMinute(0).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusHours(1).withNano(0).withSecond(0).withMinute(0);
                }
                break;
            case "day":
                if (upperDateTime.with(LocalTime.MIN).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusDays(1).with(LocalTime.MIN);
                }
                break;
            case "month":
                if (upperDateTime.with(TemporalAdjusters.firstDayOfMonth()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
                }
                break;
            case "quarter":
                if (upperDateTime.with(upperDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    LocalDateTime nextDateTime = upperDateTime.plusMonths(3);
                    truncUpperDateTime = nextDateTime.with(nextDateTime.getMonth().firstMonthOfQuarter())
                            .with(TemporalAdjusters.firstDayOfMonth());
                }
                break;
            case "year":
                if (upperDateTime.with(TemporalAdjusters.firstDayOfYear()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusYears(1).with(TemporalAdjusters.firstDayOfYear());
                }
                break;
            default:
                throw new SemanticException("Do not support date_trunc format string:{}", granularity);
        }
        return truncUpperDateTime;
    }

    private static LocalDateTime getLowerDateTime(LocalDateTime lowerDateTime, String granularity) {
        LocalDateTime truncLowerDateTime;
        switch (granularity) {
            case "minute":
                truncLowerDateTime = lowerDateTime.withNano(0).withSecond(0);
                break;
            case "hour":
                truncLowerDateTime = lowerDateTime.withNano(0).withSecond(0).withMinute(0);
                break;
            case "day":
                truncLowerDateTime = lowerDateTime.with(LocalTime.MIN);
                break;
            case "month":
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfMonth());
                break;
            case "quarter":
                truncLowerDateTime = lowerDateTime.with(lowerDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());
                break;
            case "year":
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfYear());
                break;
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + granularity);
        }
        return truncLowerDateTime;
    }

    public static Map<String, Range<PartitionKey>> diffRange(Map<String, Range<PartitionKey>> srcRangeMap,
                                                      Map<String, Range<PartitionKey>> dstRangeMap) {

        Map<String, Range<PartitionKey>> result = Maps.newHashMap();

        LinkedHashMap<String, Range<PartitionKey>> srcRangeLinkMap = srcRangeMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(RangeUtils.RANGE_COMPARATOR))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        LinkedHashMap<String, Range<PartitionKey>> dstRangeLinkMap = dstRangeMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(RangeUtils.RANGE_COMPARATOR))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeLinkMap.entrySet()) {
            boolean found = false;
            Iterator<Range<PartitionKey>> dstIter = dstRangeLinkMap.values().iterator();
            while (dstIter.hasNext()) {
                Range<PartitionKey> dstRange = dstIter.next();
                int lowerCmp = srcEntry.getValue().lowerEndpoint().compareTo(dstRange.lowerEndpoint());
                int upperCmp = srcEntry.getValue().upperEndpoint().compareTo(dstRange.upperEndpoint());
                // must be same range
                if (lowerCmp == 0 && upperCmp == 0) {
                    dstIter.remove();
                    found = true;
                    break;
                }
            }
            if (!found) {
                result.put(srcEntry.getKey(), srcEntry.getValue());
            }
        }
        return result;
    }

}
