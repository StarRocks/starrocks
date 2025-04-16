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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.mv.MVRangePartitionMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.FunctionSet.WEEK;
import static com.starrocks.sql.common.PRangeCellPlus.toPRangeCellPlus;
import static com.starrocks.sql.common.TimeUnitUtils.DAY;
import static com.starrocks.sql.common.TimeUnitUtils.HOUR;
import static com.starrocks.sql.common.TimeUnitUtils.MINUTE;
import static com.starrocks.sql.common.TimeUnitUtils.MONTH;
import static com.starrocks.sql.common.TimeUnitUtils.QUARTER;
import static com.starrocks.sql.common.TimeUnitUtils.YEAR;

/**
 * Process lower bound and upper bound for Expression Partition,
 * only support SlotRef and FunctionCallExpr
 */
public class SyncPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(SyncPartitionUtils.class);

    private SyncPartitionUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class");
    }

    private static final String DEFAULT_PREFIX = "p";

    public static PartitionDiff getRangePartitionDiffOfSlotRef(Map<String, Range<PartitionKey>> baseRangeMap,
                                                               Map<String, Range<PartitionKey>> mvRangeMap,
                                                               RangePartitionDiffer differ) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        RangeSet<PartitionKey> ranges = TreeRangeSet.create();
        Map<String, Range<PartitionKey>> unique = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> entry : baseRangeMap.entrySet()) {
            if (!ranges.encloses(entry.getValue())) {
                ranges.add(entry.getValue());
                unique.put(entry.getKey(), entry.getValue());
            }
        }
        return differ != null ? differ.diff(unique, mvRangeMap) :
                RangePartitionDiffer.simpleDiff(unique, mvRangeMap);
    }

    public static boolean hasRangePartitionChanged(Map<String, Range<PartitionKey>> baseRangeMap,
                                                   Map<String, Range<PartitionKey>> mvRangeMap) {
        PartitionDiff diff = RangePartitionDiffer.simpleDiff(baseRangeMap, mvRangeMap);
        if (MapUtils.isNotEmpty(diff.getAdds()) || MapUtils.isNotEmpty(diff.getDeletes())) {
            return true;
        }
        return false;
    }

    public static PartitionDiff getRangePartitionDiffOfExpr(Map<String, Range<PartitionKey>> baseRangeMap,
                                                            Map<String, Range<PartitionKey>> mvRangeMap,
                                                            FunctionCallExpr functionCallExpr,
                                                            RangePartitionDiffer differ) {
        PrimitiveType partitionColumnType = functionCallExpr.getType().getPrimitiveType();
        Map<String, Range<PartitionKey>> rollupRange = Maps.newHashMap();
        if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
            rollupRange = toMappingRanges(baseRangeMap, granularity, partitionColumnType);
        } else if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
            rollupRange = mappingRangeListForDate(baseRangeMap);
        }
        return getRangePartitionDiff(mvRangeMap, rollupRange, differ);
    }

    public static Map<String, Range<PartitionKey>> toMappingRanges(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                   String granularity,
                                                                   PrimitiveType partitionType) {
        MVRangePartitionMapper mapper = MVRangePartitionMapper.getInstance(granularity);
        return mapper.toMappingRanges(baseRangeMap, granularity, partitionType);
    }

    private static Map<String, Range<PartitionKey>> mappingRangeListForDate(
            Map<String, Range<PartitionKey>> baseRangeMap) {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
            Range<PartitionKey> dateRange = convertToDatePartitionRange(rangeEntry.getValue());
            DateLiteral lowerDate = (DateLiteral) dateRange.lowerEndpoint().getKeys().get(0);
            DateLiteral upperDate = (DateLiteral) dateRange.upperEndpoint().getKeys().get(0);
            String mvPartitionName = getMVPartitionName(lowerDate.toLocalDateTime(), upperDate.toLocalDateTime());

            result.put(mvPartitionName, dateRange);
        }

        return result;
    }

    @NotNull
    private static PartitionDiff getRangePartitionDiff(Map<String, Range<PartitionKey>> mvRangeMap,
                                                       Map<String, Range<PartitionKey>> rollupRange,
                                                       RangePartitionDiffer differ) {
        // TODO: Callers may use `List<PartitionRange>` directly.
        PartitionDiff diff = differ != null ? differ.diff(rollupRange, mvRangeMap) :
                RangePartitionDiffer.simpleDiff(rollupRange, mvRangeMap);
        return diff;
    }

    public static PartitionKey toPartitionKey(LocalDateTime dateTime, PrimitiveType type) throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        if (type == PrimitiveType.DATE) {
            partitionKey.pushColumn(new DateLiteral(dateTime, Type.DATE), type);
        } else {
            partitionKey.pushColumn(new DateLiteral(dateTime, Type.DATETIME), type);
        }
        return partitionKey;
    }

    public static Range<PartitionKey> convertToDatePartitionRange(Range<PartitionKey> range) {
        LiteralExpr lower = range.lowerEndpoint().getKeys().get(0);
        LiteralExpr upper = range.upperEndpoint().getKeys().get(0);
        if (!(lower instanceof StringLiteral)) {
            return range;
        }
        LocalDateTime lowerDate = DateUtils.parseStrictDateTime(lower.getStringValue());
        LocalDateTime upperDate = DateUtils.parseStrictDateTime(upper.getStringValue());
        try {
            PartitionKey lowerPartitionKey = new PartitionKey();
            PartitionKey upperPartitionKey = new PartitionKey();
            lowerPartitionKey.pushColumn(new DateLiteral(lowerDate, Type.DATE), PrimitiveType.DATE);
            upperPartitionKey.pushColumn(new DateLiteral(upperDate, Type.DATE), PrimitiveType.DATE);
            return Range.closedOpen(lowerPartitionKey, upperPartitionKey);
        } catch (AnalysisException e) {
            throw new SemanticException("Convert to DateLiteral failed:", e);
        }
    }

    /**
     * Convert base table with partition expression with the associated partition expressions.
     * eg: Create MV mv1
     * partition by tbl1.dt
     * as select * from tbl1 join on tbl2 on tbl1.dt = date_trunc('month', tbl2.dt)
     * This method will format tbl1's range partition key directly, and will format tbl2's partition range key by
     * using `date_trunc('month', tbl2.dt)`.
     * TODO: now `date_trunc` is supported, should support like to_date(ds) + 1 day ?
     */
    public static Range<PartitionKey> transferRange(Range<PartitionKey> baseRange,
                                                    Expr partitionExpr) {
        if (!(partitionExpr instanceof FunctionCallExpr)) {
            return baseRange;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
        if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
            return baseRange;
        }
        if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            throw new SemanticException("Do not support function: %s", functionCallExpr.getFnName().getFunction());
        }

        String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
        // assume expr partition must be DateLiteral and only one partition
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        Preconditions.checkArgument(lowerExpr instanceof DateLiteral);
        DateLiteral lowerDate = (DateLiteral) lowerExpr;
        LocalDateTime lowerDateTime = lowerDate.toLocalDateTime();
        LocalDateTime truncLowerDateTime = getLowerDateTime(lowerDateTime, granularity);

        DateLiteral upperDate;
        LocalDateTime truncUpperDateTime;
        if (upperExpr instanceof MaxLiteral) {
            upperDate = new DateLiteral(Type.DATE, true);
            truncUpperDateTime = upperDate.toLocalDateTime();
        } else {
            upperDate = (DateLiteral) upperExpr;
            truncUpperDateTime = getUpperDateTime(upperDate.toLocalDateTime(), granularity);
        }

        Preconditions.checkState(baseRange.lowerEndpoint().getTypes().size() == 1);
        PrimitiveType partitionType = baseRange.lowerEndpoint().getTypes().get(0);

        PartitionKey lowerPartitionKey = new PartitionKey();
        PartitionKey upperPartitionKey = new PartitionKey();
        try {
            if (partitionType == PrimitiveType.DATE) {
                lowerPartitionKey.pushColumn(new DateLiteral(truncLowerDateTime, Type.DATE), partitionType);
                upperPartitionKey.pushColumn(new DateLiteral(truncUpperDateTime, Type.DATE), partitionType);
            } else {
                lowerPartitionKey.pushColumn(new DateLiteral(truncLowerDateTime, Type.DATETIME), partitionType);
                upperPartitionKey.pushColumn(new DateLiteral(truncUpperDateTime, Type.DATETIME), partitionType);
            }
        } catch (AnalysisException e) {
            throw new SemanticException("Convert partition with date_trunc expression to date failed, lower:%s, upper:%s",
                    truncLowerDateTime, truncUpperDateTime);
        }
        return Range.closedOpen(lowerPartitionKey, upperPartitionKey);
    }

    /**
     * return all src partition name to intersected dst partition names which the src partition
     * is intersected with dst partitions.
     */
    public static Map<String, Set<String>> getIntersectedPartitions(Map<String, Range<PartitionKey>> srcRangeMap,
                                                                    Map<String, Range<PartitionKey>> dstRangeMap) {
        if (dstRangeMap.isEmpty()) {
            return srcRangeMap.keySet().stream().collect(Collectors.toMap(Function.identity(), Sets::newHashSet));
        }

        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PRangeCellPlus> srcRanges = toPRangeCellPlus(srcRangeMap, true);
        List<PRangeCellPlus> dstRanges = toPRangeCellPlus(dstRangeMap, true);
        return getIntersectedPartitions(srcRanges, dstRanges);
    }

    private static boolean isCompatibleType(PrimitiveType srcType, PrimitiveType dstType) {
        // date type is compatible with a datetime type
        // eg:
        // t1: CREATE TABLE t1 (dt DATE, num INT) PARTITION BY RANGE COLUMNS (dt);
        //
        // CREATE MATERIALIZED VIEW mv1 PARTITION BY date_trunc("month", dt1) REFRESH MANUAL
        // AS SELECT time_slice(dt, interval 5 day) as dt1,sum(num) FROM t1 GROUP BY dt1;
        //
        // base's type: DATE
        // mv's type: DATETIME
        if (srcType.isDateType() && dstType.isDateType()) {
            return true;
        }
        return srcType.equals(dstType);
    }

    /**
     * @param srcRanges : src partition ranges
     * @param dstRanges : dst partition ranges
     * @return : return all src partition name to intersected dst partition names which the src partition
     * is intersected with dst ranges.
     */
    public static Map<String, Set<String>> getIntersectedPartitions(List<PRangeCellPlus> srcRanges,
                                                                    List<PRangeCellPlus> dstRanges) {
        if (!srcRanges.isEmpty() && !dstRanges.isEmpty()) {
            PRangeCell srcRangeCell0 = srcRanges.get(0).getCell();
            PRangeCell dstRangeCell0 = dstRanges.get(0).getCell();
            List<PrimitiveType> srcTypes = srcRangeCell0.getRange().lowerEndpoint().getTypes();
            List<PrimitiveType> dstTypes = dstRangeCell0.getRange().lowerEndpoint().getTypes();
            int len = Math.min(srcTypes.size(), dstTypes.size());
            for (int i = 0; i < len; i++) {
                if (!isCompatibleType(srcTypes.get(i), dstTypes.get(i))) {
                    throw new SemanticException(String.format("src type %s must be identical to dst type %s", srcTypes.get(i),
                            dstTypes.get(i)));
                }
            }
        }

        Map<String, Set<String>> result = srcRanges.stream().collect(
                Collectors.toMap(PRangeCellPlus::getPartitionName, x -> Sets.newHashSet()));

        Collections.sort(srcRanges, PRangeCellPlus::compareTo);
        Collections.sort(dstRanges, PRangeCellPlus::compareTo);

        for (PRangeCellPlus srcRange : srcRanges) {
            int mid = Collections.binarySearch(dstRanges, srcRange);
            if (mid < 0) {
                continue;
            }
            Set<String> addedSet = result.get(srcRange.getPartitionName());
            addedSet.add(dstRanges.get(mid).getPartitionName());

            int lower = mid - 1;
            while (lower >= 0 && dstRanges.get(lower).isIntersected(srcRange)) {
                addedSet.add(dstRanges.get(lower).getPartitionName());
                lower--;
            }

            int higher = mid + 1;
            while (higher < dstRanges.size() && dstRanges.get(higher).isIntersected(srcRange)) {
                addedSet.add(dstRanges.get(higher).getPartitionName());
                higher++;
            }
        }
        return result;
    }

    public static void calcPotentialRefreshPartition(Set<String> needRefreshMvPartitionNames,
                                                     Map<Table, Set<String>> baseChangedPartitionNames,
                                                     Map<Table, Map<String, Set<String>>> baseToMvNameRef,
                                                     Map<String, Map<Table, Set<String>>> mvToBaseNameRef,
                                                     Set<String> mvPotentialRefreshPartitionNames) {
        gatherPotentialRefreshPartitionNames(needRefreshMvPartitionNames, baseChangedPartitionNames,
                baseToMvNameRef, mvToBaseNameRef, mvPotentialRefreshPartitionNames);
    }

    private static void gatherPotentialRefreshPartitionNames(Set<String> needRefreshMvPartitionNames,
                                                             Map<Table, Set<String>> baseChangedPartitionNames,
                                                             Map<Table, Map<String, Set<String>>> baseToMvNameRef,
                                                             Map<String, Map<Table, Set<String>>> mvToBaseNameRef,
                                                             Set<String> mvPotentialRefreshPartitionNames) {
        int curNameCount = needRefreshMvPartitionNames.size();
        Set<String> copiedNeedRefreshMvPartitionNames = Sets.newHashSet(needRefreshMvPartitionNames);
        for (String needRefreshMvPartitionName : copiedNeedRefreshMvPartitionNames) {
            // baseTable with its partitions by mv's partition
            Map<Table, Set<String>> baseNames = mvToBaseNameRef.get(needRefreshMvPartitionName);
            if (baseNames == null) {
                // mv partition has no base table partition reference if its partition is not added since
                LOG.warn("MV partition {} does not existed in the collected mv to base table partition mapping: {}",
                        needRefreshMvPartitionName, mvToBaseNameRef);
                continue;
            }
            Set<String> mvNeedRefreshPartitions = Sets.newHashSet();
            for (Map.Entry<Table, Set<String>> entry : baseNames.entrySet()) {
                Table baseTable = entry.getKey();
                Set<String> baseTablePartitions = entry.getValue();
                // base table partition with associated mv's partitions
                Map<String, Set<String>> baseTableToMVPartitionsMap = baseToMvNameRef.get(baseTable);
                for (String baseTablePartition : baseTablePartitions) {
                    // find base table partition associated mv partition names
                    Set<String> mvAssociatedPartitions = baseTableToMVPartitionsMap.get(baseTablePartition);
                    mvNeedRefreshPartitions.addAll(mvAssociatedPartitions);
                }

                if (mvNeedRefreshPartitions.size() > 1) {
                    needRefreshMvPartitionNames.addAll(mvNeedRefreshPartitions);
                    mvPotentialRefreshPartitionNames.add(needRefreshMvPartitionName);
                    baseChangedPartitionNames.computeIfAbsent(baseTable, x -> Sets.newHashSet())
                            .addAll(baseTablePartitions);
                }
            }
        }

        if (curNameCount != needRefreshMvPartitionNames.size()) {
            gatherPotentialRefreshPartitionNames(needRefreshMvPartitionNames, baseChangedPartitionNames,
                    baseToMvNameRef, mvToBaseNameRef, mvPotentialRefreshPartitionNames);
        }
    }

    public static String getMVPartitionName(Expr mvPartitionExpr, Range<PartitionKey> range) {
        Type partitionType = mvPartitionExpr.getType();
        DateLiteral upperDate = (DateLiteral) range.upperEndpoint().getKeys().get(0);
        DateLiteral lowerDate = (DateLiteral) range.lowerEndpoint().getKeys().get(0);
        if (partitionType.isDate()) {
            return getMVPartitionName(lowerDate.toLocalDateTime(), upperDate.toLocalDateTime());
        } else {
            // use the minimum granularity to generate the partition name
            return getMVPartitionName(lowerDate.toLocalDateTime(), upperDate.toLocalDateTime(), MINUTE);
        }
    }

    public static String getMVPartitionName(LocalDateTime lower, LocalDateTime upper) {
        return DEFAULT_PREFIX + lower.format(DateUtils.DATEKEY_FORMATTER)
                + "_" + upper.format(DateUtils.DATEKEY_FORMATTER);
    }

    public static String getMVPartitionName(LocalDateTime lowerDateTime, LocalDateTime upperDateTime,
                                            String granularity) {
        switch (granularity) {
            case MINUTE:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.MINUTE_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.MINUTE_FORMATTER);
            case HOUR:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.HOUR_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.HOUR_FORMATTER);
            case DAY:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.DATEKEY_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.DATEKEY_FORMATTER);
            case WEEK:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.DATEKEY_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.DATEKEY_FORMATTER);
            case MONTH:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.MONTH_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.MONTH_FORMATTER);
            case QUARTER:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.QUARTER_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.QUARTER_FORMATTER);
            case YEAR:
                return DEFAULT_PREFIX + lowerDateTime.format(DateUtils.YEAR_FORMATTER) +
                        "_" + upperDateTime.format(DateUtils.YEAR_FORMATTER);
            default:
                throw new SemanticException("Do not support date_trunc format string:{}", granularity);
        }
    }

    // when the upperDateTime is the same as granularity rollup time, should not +1
    @NotNull
    public static LocalDateTime getUpperDateTime(LocalDateTime upperDateTime, String granularity) {
        LocalDateTime truncUpperDateTime;
        switch (granularity) {
            case MINUTE:
                if (upperDateTime.withNano(0).withSecond(0).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusMinutes(1).withNano(0).withSecond(0);
                }
                break;
            case HOUR:
                if (upperDateTime.withNano(0).withSecond(0).withMinute(0).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusHours(1).withNano(0).withSecond(0).withMinute(0);
                }
                break;
            case DAY:
                if (upperDateTime.with(LocalTime.MIN).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusDays(1).with(LocalTime.MIN);
                }
                break;
            case WEEK:
                if (upperDateTime.with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusWeeks(1).with(LocalTime.MIN);
                }
                break;
            case MONTH:
                if (upperDateTime.with(TemporalAdjusters.firstDayOfMonth()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
                }
                break;
            case QUARTER:
                if (upperDateTime.with(upperDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    LocalDateTime nextDateTime = upperDateTime.plusMonths(3);
                    truncUpperDateTime = nextDateTime.with(nextDateTime.getMonth().firstMonthOfQuarter())
                            .with(TemporalAdjusters.firstDayOfMonth());
                }
                break;
            case YEAR:
                if (upperDateTime.with(TemporalAdjusters.firstDayOfYear()).equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = upperDateTime.plusYears(1).with(TemporalAdjusters.firstDayOfYear());
                }
                break;
            default:
                throw new SemanticException("Do not support date_trunc format string:{}", granularity);
        }
        final DateLiteral maxDateTime = DateLiteral.createMaxValue(Type.DATETIME);
        if (truncUpperDateTime.isAfter(maxDateTime.toLocalDateTime())) {
            return upperDateTime;
        }
        return truncUpperDateTime;
    }

    @NotNull
    public static LocalDateTime nextUpperDateTime(LocalDateTime upperDateTime, String granularity) {
        LocalDateTime truncUpperDateTime;
        switch (granularity) {
            case MINUTE:
                truncUpperDateTime = upperDateTime.plusMinutes(1).withNano(0).withSecond(0);
                break;
            case HOUR:
                truncUpperDateTime = upperDateTime.plusHours(1).withNano(0).withSecond(0).withMinute(0);
                break;
            case DAY:
                truncUpperDateTime = upperDateTime.plusDays(1).with(LocalTime.MIN);
                break;
            case WEEK:
                truncUpperDateTime = upperDateTime.plusWeeks(1).with(LocalTime.MIN);
                break;
            case MONTH:
                truncUpperDateTime = upperDateTime.plusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
                break;
            case QUARTER:
                LocalDateTime nextDateTime = upperDateTime.plusMonths(3);
                truncUpperDateTime = nextDateTime.with(nextDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());
                break;
            case YEAR:
                truncUpperDateTime = upperDateTime.plusYears(1).with(TemporalAdjusters.firstDayOfYear());
                break;
            default:
                throw new SemanticException("Do not support date_trunc format string:{}", granularity);
        }
        final DateLiteral maxDateTime = DateLiteral.createMaxValue(Type.DATETIME);
        if (truncUpperDateTime.isAfter(maxDateTime.toLocalDateTime())) {
            return upperDateTime;
        }
        return truncUpperDateTime;
    }

    public static LocalDateTime getLowerDateTime(LocalDateTime lowerDateTime, String granularity) {
        LocalDateTime truncLowerDateTime;
        switch (granularity) {
            case MINUTE:
                truncLowerDateTime = lowerDateTime.withNano(0).withSecond(0);
                break;
            case HOUR:
                truncLowerDateTime = lowerDateTime.withNano(0).withSecond(0).withMinute(0);
                break;
            case DAY:
                truncLowerDateTime = lowerDateTime.with(LocalTime.MIN);
                break;
            case WEEK:
                truncLowerDateTime = lowerDateTime.with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS);
                break;
            case MONTH:
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfMonth());
                break;
            case QUARTER:
                truncLowerDateTime = lowerDateTime.with(lowerDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth());
                break;
            case YEAR:
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfYear());
                break;
            default:
                throw new SemanticException("Do not support in date_trunc format string:" + granularity);
        }
        return truncLowerDateTime;
    }

    public static Range<PartitionKey> createRange(String lowerBound, String upperBound, Column partitionColumn)
            throws AnalysisException {
        if (lowerBound == null && upperBound == null) {
            return null;
        }
        PartitionValue lowerValue = new PartitionValue(lowerBound);
        PartitionValue upperValue;
        if (upperBound.equalsIgnoreCase(MaxLiteral.MAX_VALUE.toString())) {
            upperValue = PartitionValue.MAX_VALUE;
        } else {
            upperValue = new PartitionValue(upperBound);
        }
        PartitionKey lowerBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                Collections.singletonList(partitionColumn));
        PartitionKey upperBoundPartitionKey = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                Collections.singletonList(partitionColumn));
        return Range.closedOpen(lowerBoundPartitionKey, upperBoundPartitionKey);
    }

    private static void dropRefBaseTableFromVersionMap(
            MaterializedView mv,
            Map<String, MaterializedView.BasePartitionInfo> baseTableVersionInfoMap,
            Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap,
            String refBaseTable,
            String mvPartitionName) {
        Set<String> refBaseTableAssociatedPartitions =
                mvPartitionNameRefBaseTablePartitionMap.get(mvPartitionName);
        Preconditions.checkState(refBaseTableAssociatedPartitions != null);
        LOG.info("Remove ref base table {} associated partitions {} from materialized view {}'s " +
                        "version meta because materialized view's partition {} has been dropped",
                refBaseTable, Joiner.on(",").join(refBaseTableAssociatedPartitions),
                mv.getName(), mvPartitionName);
        for (String refBaseTableAssociatedPartition : refBaseTableAssociatedPartitions) {
            if (!baseTableVersionInfoMap.containsKey(refBaseTableAssociatedPartition)) {
                LOG.warn("WARNING: mvPartitionNameRefBaseTablePartitionMap {} failed to tracked the materialized view {} " +
                                "partition {}", Joiner.on(",").join(mvPartitionNameRefBaseTablePartitionMap.keySet()),
                        mv.getName(), mvPartitionName);
                continue;
            }
            baseTableVersionInfoMap.remove(refBaseTableAssociatedPartition);
        }

        // finally remove the dropped materialized view partition
        mvPartitionNameRefBaseTablePartitionMap.remove(mvPartitionName);
    }

    private static boolean isMVPartitionNameRefBaseTablePartitionMapEnough(
            Map<String, MaterializedView.BasePartitionInfo> baseTableVersionInfoMap,
            Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap) {
        long refreshedRefBaseTablePartitionSize = baseTableVersionInfoMap.keySet().size();
        long refreshAssociatedRefTablePartitionSize = mvPartitionNameRefBaseTablePartitionMap.values()
                .stream().map(Set::size).reduce(0, Integer::sum);
        return refreshedRefBaseTablePartitionSize == refreshAssociatedRefTablePartitionSize;
    }

    private static void dropRefBaseTableFromVersionMapForOlapTable(
            MaterializedView mv,
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap,
            Long tableId,
            String mvPartitionName) {
        if (!versionMap.containsKey(tableId)) {
            // version map should always contain ref base table's version info.
            LOG.warn("Base ref table {} is not found in the base table version info map when " +
                            "materialized view {} drops partition:{}",
                    tableId, mv.getName(), mvPartitionName);
            return;
        }

        Map<String, MaterializedView.BasePartitionInfo> baseTableVersionInfoMap = versionMap.get(tableId);
        Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap();
        if (mvPartitionNameRefBaseTablePartitionMap.containsKey(mvPartitionName)) {
            dropRefBaseTableFromVersionMap(mv, baseTableVersionInfoMap, mvPartitionNameRefBaseTablePartitionMap,
                    tableId.toString(), mvPartitionName);
        } else {
            if (isMVPartitionNameRefBaseTablePartitionMapEnough(baseTableVersionInfoMap,
                    mvPartitionNameRefBaseTablePartitionMap)) {
                // It's safe here that only log warning rather than remove all the ref base table info from version map,
                // because mvPartitionNameRefBaseTablePartitionMap should track all the changed materialized view
                // partitions.
                LOG.info("Skip to remove ref base table {} from materialized view {}'s version meta when " +
                                "materialized view's partition {} has been dropped because this partition is not " +
                                "in the version map",
                        tableId, mv.getName(), mvPartitionName);
            } else {
                // NOTE: If the materialized view has created in old version, mvPartitionNameRefBaseTablePartitionMap
                // may not contain enough info to track associated ref base change partitions.
                LOG.warn("Remove ref base table {} from materialized view {}'s version meta because " +
                                "materialized view's partition {} has been dropped",
                        tableId, mv.getName(), mvPartitionName);
                versionMap.remove(tableId);
            }
        }
    }

    private static void dropRefBaseTableFromVersionMapForExternalTable(
            MaterializedView mv,
            Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> versionMap,
            BaseTableInfo baseTableInfo,
            String mvPartitionName) {
        if (!versionMap.containsKey(baseTableInfo)) {
            // version map should always contain ref base table's version info.
            LOG.warn("Base ref table {} is not found in the base table version info map when " +
                            "materialized view {} drops partition:{}",
                    baseTableInfo, mv.getName(), mvPartitionName);
            return;
        }

        Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap();
        Map<String, MaterializedView.BasePartitionInfo> baseTableVersionInfoMap = versionMap.get(baseTableInfo);
        if (mvPartitionNameRefBaseTablePartitionMap.containsKey(mvPartitionName)) {
            dropRefBaseTableFromVersionMap(mv, baseTableVersionInfoMap,
                    mvPartitionNameRefBaseTablePartitionMap, baseTableInfo.getTableName(), mvPartitionName);
        } else {
            if (isMVPartitionNameRefBaseTablePartitionMapEnough(baseTableVersionInfoMap,
                    mvPartitionNameRefBaseTablePartitionMap)) {
                // It's safe here that only log warning rather than remove all the ref base table info from version map,
                // because mvPartitionNameRefBaseTablePartitionMap should track all the changed materialized view
                // partitions.
                LOG.info("Skip to remove ref base table {} from materialized view {}'s version meta when " +
                                "materialized view's partition {} has been dropped because this partition is not " +
                                "in the version map",
                        baseTableInfo, mv.getName(), mvPartitionName);
            } else {
                // NOTE: If the materialized view has created in old version, mvPartitionNameRefBaseTablePartitionMap
                // may not contain enough info to track associated ref base change partitions.
                LOG.warn("Remove ref base table {} from materialized view {}'s version meta because " +
                                "materialized view's partition {} has been dropped",
                        baseTableInfo, mv.getName(), mvPartitionName);
                versionMap.remove(baseTableInfo);
            }
        }
    }

    private static void dropBaseVersionMetaForOlapTable(MaterializedView mv, String mvPartitionName,
                                                        Range<PartitionKey> mvPartitionRange,
                                                        MaterializedView.AsyncRefreshContext refreshContext,
                                                        TableName tableName) {
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                refreshContext.getBaseTableVisibleVersionMap();
        if (versionMap == null) {
            return;
        }
        Expr expr = mv.getPartitionRefTableExprs().get(0);

        Database baseDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(tableName.getDb());
        if (baseDb == null) {
            return;
        }
        Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(baseDb.getFullName(), tableName.getTbl());
        if (baseTable == null) {
            return;
        }
        long tableId = baseTable.getId();
        if (expr instanceof FunctionCallExpr) {
            // TODO: use `dropRefBaseTableFromVersionMapForOlapTable` either.
            Map<String, MaterializedView.BasePartitionInfo> mvTableVersionMap = versionMap.get(tableId);
            if (mvTableVersionMap != null && mvPartitionRange != null && baseTable instanceof OlapTable) {
                // use range derive connect base partition
                Map<String, Range<PartitionKey>> basePartitionMap = ((OlapTable) baseTable).getRangePartitionMap();
                Map<String, Set<String>> mvToBaseMapping = getIntersectedPartitions(
                        Collections.singletonMap(mvPartitionName, mvPartitionRange), basePartitionMap);
                mvToBaseMapping.values().forEach(parts -> parts.forEach(mvTableVersionMap::remove));
            }
        } else {
            dropRefBaseTableFromVersionMapForOlapTable(mv, versionMap, tableId, mvPartitionName);
        }
    }

    private static void dropBaseVersionMetaForExternalTable(MaterializedView mv, String mvPartitionName,
                                                            MaterializedView.AsyncRefreshContext refreshContext,
                                                            TableName tableName) {
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                refreshContext.getBaseTableInfoVisibleVersionMap();
        if (versionMap == null) {
            return;
        }
        if (StringUtils.isEmpty(tableName.getCatalog()) || InternalCatalog.isFromDefault(tableName)) {
            return;
        }
        List<Expr> mvPartitionRefTableExprs = mv.getPartitionRefTableExprs();
        if (CollectionUtils.isEmpty(mvPartitionRefTableExprs)) {
            return;
        }
        // TODO: support multiple partition columns
        if (mvPartitionRefTableExprs.size() > 1) {
            return;
        }
        Expr expr = mv.getPartitionRefTableExprs().get(0);
        Table baseTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), tableName.getCatalog(), tableName.getDb(), tableName.getTbl());

        if (baseTable == null) {
            return;
        }
        if (expr instanceof SlotRef) {
            // TODO: use `dropRefBaseTableFromVersionMapForExternalTable` later.
            Column partitionColumn = baseTable.getColumn(((SlotRef) expr).getColumnName());
            BaseTableInfo baseTableInfo = new BaseTableInfo(tableName.getCatalog(), tableName.getDb(),
                    baseTable.getName(), baseTable.getTableIdentifier());
            Map<String, MaterializedView.BasePartitionInfo> baseTableVersionMap = versionMap.get(baseTableInfo);
            if (baseTableVersionMap != null) {
                baseTableVersionMap.keySet().removeIf(partitionName -> {
                    try {
                        boolean isListPartition = mv.getPartitionInfo().isListPartition();
                        Set<String> partitionNames = PartitionUtil.getMVPartitionName(baseTable, partitionColumn,
                                Lists.newArrayList(partitionName), isListPartition, expr);
                        return partitionNames != null && partitionNames.size() == 1 &&
                                Lists.newArrayList(partitionNames).get(0).equals(mvPartitionName);
                    } catch (AnalysisException e) {
                        LOG.warn("failed to get mv partition name", e);
                        return false;
                    }
                });
            }
        } else {
            BaseTableInfo baseTableInfo = new BaseTableInfo(tableName.getCatalog(), tableName.getDb(),
                    baseTable.getName(), baseTable.getTableIdentifier());
            dropRefBaseTableFromVersionMapForExternalTable(mv, versionMap, baseTableInfo, mvPartitionName);
        }
    }

    public static void dropBaseVersionMeta(MaterializedView mv, String mvPartitionName,
                                           Range<PartitionKey> partitionRange) {
        MaterializedView.AsyncRefreshContext refreshContext = mv.getRefreshScheme().getAsyncRefreshContext();

        Expr expr = mv.getPartitionRefTableExprs().get(0);
        SlotRef slotRef;
        if (expr instanceof SlotRef) {
            slotRef = (SlotRef) expr;
        } else {
            List<SlotRef> slotRefs = Lists.newArrayList();
            expr.collect(SlotRef.class, slotRefs);
            slotRef = slotRefs.get(0);
        }
        TableName tableName = slotRef.getTblNameWithoutAnalyzed();
        // base version meta for olap table and external table are different, we need to drop them separately
        dropBaseVersionMetaForOlapTable(mv, mvPartitionName, partitionRange, refreshContext, tableName);
        dropBaseVersionMetaForExternalTable(mv, mvPartitionName, refreshContext, tableName);
    }
}
