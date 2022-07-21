// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;


import com.clearspring.analytics.util.Lists;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
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
            // this will cause range overlap
            timePointSet.add(mappedRange.getFirstTime());
            timePointSet.add(mappedRange.getLastTime());
        }
        List<LocalDateTime> timePointList = Lists.newArrayList(timePointSet);
        // deal overlap
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
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
                throw new SemanticException("Convert to DateLiteral failed:" + ex);
            }
        }
        return result;
    }

    public static PartitionMapping mappingRange(Range<PartitionKey> baseRange,
                                                                String granularity) {
        // assume expr partition must be DateLiteral and only one partition
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        Preconditions.checkArgument(lowerExpr instanceof DateLiteral);
        Preconditions.checkArgument(upperExpr instanceof DateLiteral);
        DateLiteral lowerDate = (DateLiteral) lowerExpr;
        DateLiteral upperDate = (DateLiteral) upperExpr;
        LocalDateTime lowerDateTime = lowerDate.toLocalDateTime();
        LocalDateTime upperDateTime = upperDate.toLocalDateTime();
//        PrimitiveType srcType = baseRange.upperEndpoint().getTypes().get(0);

        LocalDateTime firstTime = getFirstTime(lowerDateTime, granularity);
        LocalDateTime lastTime = getLastTime(upperDateTime, granularity);
        return new PartitionMapping(firstTime, lastTime);
    }

    public static String getMVPartitionName(LocalDateTime firstTime, LocalDateTime lastTime, String granularity) {
        switch (granularity) {
            case "minute":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.MINUTE_FORMATTER) +
                        "_" + lastTime.format(DateUtils.MINUTE_FORMATTER);
            case "hour":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.HOUR_FORMATTER) +
                        "_" + lastTime.format(DateUtils.HOUR_FORMATTER);
            case "day":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.DATEKEY_FORMATTER) +
                        "_" + lastTime.format(DateUtils.DATEKEY_FORMATTER);
            case "month":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.MONTH_FORMATTER) +
                        "_" + lastTime.format(DateUtils.MONTH_FORMATTER);
            case "quarter":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.QUARTER_FORMATTER) +
                        "_" + lastTime.format(DateUtils.QUARTER_FORMATTER);
            case "year":
                return DEFAULT_PREFIX + firstTime.format(DateUtils.YEAR_FORMATTER) +
                        "_" + lastTime.format(DateUtils.YEAR_FORMATTER);
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + granularity);
        }
    }

    @NotNull
    private static LocalDateTime getLastTime(LocalDateTime upperDateTime, String granularity) {
        LocalDateTime lastTime;
        switch (granularity) {
            case "minute":
                lastTime = upperDateTime.plusMinutes(1).withNano(0).withSecond(0);;
                break;
            case "hour":
                lastTime = upperDateTime.plusHours(1).withNano(0).withSecond(0).withMinute(0);;
                break;
            case "day":
                lastTime = upperDateTime.plusDays(1).with(LocalTime.MIN);
                break;
            case "month":
                lastTime = upperDateTime.plusMonths(1).with(TemporalAdjusters.firstDayOfMonth());;
                break;
            case "quarter":
                LocalDateTime nextDateTime = upperDateTime.plusMonths(3);
                lastTime = nextDateTime.with(nextDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());;
                break;
            case "year":
                lastTime = upperDateTime.plusYears(1).with(TemporalAdjusters.firstDayOfYear());;
                break;
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + granularity);
        }
        return lastTime;
    }

    private static LocalDateTime getFirstTime(LocalDateTime lowerDateTime, String granularity) {
        LocalDateTime firstTime;
        switch (granularity) {
            case "minute":
                firstTime = lowerDateTime.withNano(0).withSecond(0);
                break;
            case "hour":
                firstTime = lowerDateTime.withNano(0).withSecond(0).withMinute(0);
                break;
            case "day":
                firstTime = lowerDateTime.with(LocalTime.MIN);
                break;
            case "month":
                firstTime = lowerDateTime.with(TemporalAdjusters.firstDayOfMonth());
                break;
            case "quarter":
                firstTime = lowerDateTime.with(lowerDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());
                break;
            case "year":
                firstTime = lowerDateTime.with(TemporalAdjusters.firstDayOfYear());
                break;
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + granularity);
        }
        return firstTime;
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
