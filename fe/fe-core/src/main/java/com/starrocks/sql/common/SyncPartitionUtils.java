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
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.mv.MVRangePartitionMapper;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.apache.commons.collections4.CollectionUtils;
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

    public static PartitionDiff getRangePartitionDiffOfSlotRef(PCellSortedSet baseRangeMap,
                                                               PCellSortedSet mvRangeMap,
                                                               RangePartitionDiffer differ) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        RangeSet<PartitionKey> ranges = TreeRangeSet.create();
        PCellSortedSet unique = PCellSortedSet.of();
        for (PCellWithName entry : baseRangeMap.getPartitions()) {
            PRangeCell rangeCell = entry.cell().cast();
            if (!ranges.encloses(rangeCell.getRange())) {
                ranges.add(rangeCell.getRange());
                unique.add(entry);
            }
        }
        return differ != null ? differ.diff(unique, mvRangeMap) :
                RangePartitionDiffer.simpleDiff(unique, mvRangeMap);
    }

    public static boolean hasRangePartitionChanged(PCellSortedSet baseRangeMap,
                                                   PCellSortedSet mvRangeMap) {
        PartitionDiff diff = RangePartitionDiffer.simpleDiff(baseRangeMap, mvRangeMap);
        if (!diff.getAdds().isEmpty() || !diff.getDeletes().isEmpty()) {
            return true;
        }
        return false;
    }

    public static PartitionDiff getRangePartitionDiffOfExpr(PCellSortedSet baseRangeMap,
                                                            PCellSortedSet mvRangeMap,
                                                            FunctionCallExpr functionCallExpr,
                                                            RangePartitionDiffer differ) {
        PrimitiveType partitionColumnType = functionCallExpr.getType().getPrimitiveType();
        PCellSortedSet rollupRange = PCellSortedSet.of();
        if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
            rollupRange = toMappingRanges(baseRangeMap, granularity, partitionColumnType);
        } else if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
            rollupRange = mappingRangeListForDate(baseRangeMap);
        }
        return getRangePartitionDiff(mvRangeMap, rollupRange, differ);
    }

    public static PCellSortedSet toMappingRanges(PCellSortedSet baseRangeMap,
                                                 String granularity,
                                                 PrimitiveType partitionType) {
        MVRangePartitionMapper mapper = MVRangePartitionMapper.getInstance(granularity);
        return mapper.toMappingRanges(baseRangeMap, granularity, partitionType);
    }

    private static PCellSortedSet mappingRangeListForDate(PCellSortedSet baseRangeMap) {
        PCellSortedSet result = PCellSortedSet.of();
        for (PCellWithName rangeEntry : baseRangeMap.getPartitions()) {
            PRangeCell rangeCell = rangeEntry.cell().cast();
            Range<PartitionKey> dateRange = convertToDatePartitionRange(rangeCell.getRange());
            DateLiteral lowerDate = (DateLiteral) dateRange.lowerEndpoint().getKeys().get(0);
            DateLiteral upperDate = (DateLiteral) dateRange.upperEndpoint().getKeys().get(0);
            String mvPartitionName = getMVPartitionName(lowerDate.toLocalDateTime(), upperDate.toLocalDateTime());
            result.add(mvPartitionName, new PRangeCell(dateRange));
        }

        return result;
    }

    @NotNull
    private static PartitionDiff getRangePartitionDiff(PCellSortedSet mvRangeMap,
                                                       PCellSortedSet rollupRange,
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
        Preconditions.checkState(baseRange.lowerEndpoint().getTypes().size() == 1);

        String granularity = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
        // assume expr partition must be DateLiteral and only one partition
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);

        PrimitiveType partitionType = baseRange.lowerEndpoint().getTypes().get(0);
        PartitionKey lowerPartitionKey = new PartitionKey();
        PartitionKey upperPartitionKey = new PartitionKey();
        try {
            DateLiteral lowerDate = transferDateLiteral(lowerExpr, granularity, true);
            DateLiteral upperDate = transferDateLiteral(upperExpr, granularity, false);
            lowerPartitionKey.pushColumn(lowerDate, partitionType);
            upperPartitionKey.pushColumn(upperDate, partitionType);
        } catch (AnalysisException e) {
            throw new SemanticException("Convert partition with date_trunc expression to date failed, lower:%s, upper:%s",
                    lowerExpr, upperExpr);
        }
        return Range.closedOpen(lowerPartitionKey, upperPartitionKey);
    }

    /**
     * Transfer date literal to the lower or upper key of the partition range.
     * @param literalExpr: the date literal to be transferred
     * @param granularity: the granularity of the partition, such as "day", "month", etc.
     * @param isLowerKey: if true, transfer to the lower key of the partition range,
     * @return the transferred date literal
     * @throws AnalysisException: if the literalExpr is not a date or datetime type,
     */
    private static DateLiteral transferDateLiteral(LiteralExpr literalExpr,
                                                   String granularity,
                                                   boolean isLowerKey) throws AnalysisException {
        if (literalExpr == null) {
            return null;
        }
        if (literalExpr.getType() != Type.DATE && literalExpr.getType() != Type.DATETIME) {
            throw new SemanticException("Do not support date_trunc for type: %s", literalExpr.getType());
        }
        DateLiteral dateLiteral = (DateLiteral) literalExpr;
        if (dateLiteral.isMinValue()) {
            return dateLiteral;
        } else if (literalExpr instanceof MaxLiteral) {
            return dateLiteral;
        }
        LocalDateTime dateTime = dateLiteral.toLocalDateTime();
        LocalDateTime localDateTime;
        if (isLowerKey) {
            localDateTime = getLowerDateTime(dateTime, granularity);
        } else {
            localDateTime = getUpperDateTime(dateTime, granularity);
        }
        return new DateLiteral(localDateTime, literalExpr.getType());
    }

    /**
     * return all src partition name to intersected dst partition names which the src partition
     * is intersected with dst partitions.
     */
    public static PartitionNameSetMap getIntersectedPartitions(PCellSortedSet srcRangeMap,
                                                               PCellSortedSet dstRangeMap) {
        if (dstRangeMap.isEmpty()) {
            return PartitionNameSetMap.of(srcRangeMap
                    .stream()
                    .map(PCellWithName::name)
                    .collect(Collectors.toMap(Function.identity(), Sets::newHashSet)));
        }

        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PCellWithName> srcRanges = srcRangeMap.getPartitions().stream().toList();
        List<PCellWithName> dstRanges = dstRangeMap.getPartitions().stream().toList();
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
    public static PartitionNameSetMap getIntersectedPartitions(List<PCellWithName> srcRanges,
                                                               List<PCellWithName> dstRanges) {
        if (!srcRanges.isEmpty() && !dstRanges.isEmpty()) {
            PRangeCell srcRangeCell0 = srcRanges.get(0).cell().cast();
            PRangeCell dstRangeCell0 = dstRanges.get(0).cell().cast();
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

        PartitionNameSetMap result = PartitionNameSetMap.of(srcRanges.stream().collect(
                Collectors.toMap(PCellWithName::name, x -> Sets.newHashSet())));

        List<PartitionKey> lowerPoints = dstRanges.stream()
                .map(pCell -> (PRangeCell) pCell.cell())
                .map(dstRange -> dstRange.getRange().lowerEndpoint())
                .toList();
        List<PartitionKey> upperPoints = dstRanges.stream().map(
                dstRange -> ((PRangeCell) dstRange.cell()).getRange().upperEndpoint()).toList();
        for (PCellWithName srcRange : srcRanges) {
            PartitionKey lower = ((PRangeCell) srcRange.cell()).getRange().lowerEndpoint();
            PartitionKey upper = ((PRangeCell) srcRange.cell()).getRange().upperEndpoint();

            // For an interval [l, r], if there exists another interval [li, ri] that intersects with it, this interval
            // must satisfy l ≤ ri and r ≥ li. Therefore, if there exists a pos_a such that for all k < pos_a,
            // ri[k] < l, and there exists a pos_b such that for all k > pos_b, li[k] > r, then all intervals between
            // pos_a and pos_b might potentially intersect with the interval [l, r].
            int posA = PartitionKey.findLastLessEqualInOrderedList(lower, upperPoints);
            int posB = PartitionKey.findLastLessEqualInOrderedList(upper, lowerPoints);

            Set<String> addedSet = result.get(srcRange.name());
            for (int i = posA; i <= posB; ++i) {
                if (dstRanges.get(i).cell().isIntersected(srcRange.cell())) {
                    addedSet.add(dstRanges.get(i).name());
                }
            }
        }
        return result;
    }

    public static void calcPotentialRefreshPartition(PCellSortedSet mvToRefreshPartitionNames,
                                                     Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                     Map<Table, PCellSetMapping> baseToMvNameRef,
                                                     Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRef,
                                                     PCellSortedSet mvPotentialRefreshPartitionNames) {
        gatherPotentialRefreshPartitionNames(mvToRefreshPartitionNames, baseChangedPartitionNames,
                baseToMvNameRef, mvToBaseNameRef, mvPotentialRefreshPartitionNames);
    }

    private static void gatherPotentialRefreshPartitionNames(PCellSortedSet mvToRefreshPartitionNames,
                                                             Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                             Map<Table, PCellSetMapping> baseToMvNameRef,
                                                             Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRef,
                                                             PCellSortedSet mvPotentialRefreshPartitionNames) {
        int curNameCount = mvToRefreshPartitionNames.size();
        PCellSortedSet copiedNeedRefreshMvPartitionNames = PCellSortedSet.of(mvToRefreshPartitionNames);
        for (PCellWithName pCellWithName : copiedNeedRefreshMvPartitionNames.getPartitions()) {
            String needRefreshMvPartitionName = pCellWithName.name();
            // baseTable with its partitions by mv's partition
            Map<Table, PCellSortedSet> baseNames = mvToBaseNameRef.get(needRefreshMvPartitionName);
            if (baseNames == null) {
                // mv partition has no base table partition reference if its partition is not added since
                LOG.warn("MV partition {} does not existed in the collected mv to base table partition mapping: {}",
                        needRefreshMvPartitionName, mvToBaseNameRef);
                continue;
            }
            PCellSortedSet mvNeedRefreshPartitions = PCellSortedSet.of();
            for (Map.Entry<Table, PCellSortedSet> entry : baseNames.entrySet()) {
                Table baseTable = entry.getKey();
                PCellSortedSet baseTablePartitions = entry.getValue();
                // base table partition with associated mv's partitions
                PCellSetMapping baseTableToMVPartitionsMap = baseToMvNameRef.get(baseTable);
                for (PCellWithName pCell : baseTablePartitions.getPartitions()) {
                    String baseTablePartitionName = pCell.name();
                    // find base table partition associated mv partition names
                    PCellSortedSet mvAssociatedPartitions = baseTableToMVPartitionsMap.get(baseTablePartitionName);
                    mvNeedRefreshPartitions.addAll(mvAssociatedPartitions);
                }

                if (mvNeedRefreshPartitions.size() > 1) {
                    mvToRefreshPartitionNames.addAll(mvNeedRefreshPartitions);
                    mvPotentialRefreshPartitionNames.add(pCellWithName);
                    baseChangedPartitionNames.computeIfAbsent(baseTable, x -> PCellSortedSet.of())
                            .addAll(baseTablePartitions);
                }
            }
        }

        if (curNameCount != mvToRefreshPartitionNames.size()) {
            gatherPotentialRefreshPartitionNames(mvToRefreshPartitionNames, baseChangedPartitionNames,
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
                LocalDateTime weekStart = upperDateTime.with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS);
                if (weekStart.equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = weekStart.plusWeeks(1).with(LocalTime.MIN);
                }
                break;
            case MONTH:
                LocalDateTime monthStart = upperDateTime.with(TemporalAdjusters.firstDayOfMonth()).with(LocalTime.MIDNIGHT);
                if (monthStart.equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = monthStart.plusMonths(1);
                }
                break;
            case QUARTER:
                LocalDateTime quarterStart = upperDateTime
                        .with(upperDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth())
                        .with(LocalTime.MIDNIGHT);
                if (quarterStart.equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = quarterStart.plusMonths(3);
                }
                break;
            case YEAR:
                LocalDateTime yearStart = upperDateTime.with(TemporalAdjusters.firstDayOfYear()).with(LocalTime.MIDNIGHT);
                if (yearStart.equals(upperDateTime)) {
                    truncUpperDateTime = upperDateTime;
                } else {
                    truncUpperDateTime = yearStart.plusYears(1);
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
                truncUpperDateTime = upperDateTime.plusMonths(1).with(TemporalAdjusters.firstDayOfMonth())
                        .with(LocalTime.MIDNIGHT);
                break;
            case QUARTER:
                LocalDateTime nextDateTime = upperDateTime.plusMonths(3);
                truncUpperDateTime = nextDateTime.with(nextDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth()).with(LocalTime.MIDNIGHT);
                break;
            case YEAR:
                truncUpperDateTime = upperDateTime.plusYears(1).with(TemporalAdjusters.firstDayOfYear())
                        .with(LocalTime.MIDNIGHT);
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
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfMonth()).with(LocalTime.MIDNIGHT);
                break;
            case QUARTER:
                truncLowerDateTime = lowerDateTime.with(lowerDateTime.getMonth().firstMonthOfQuarter())
                        .with(TemporalAdjusters.firstDayOfMonth())
                        .with(LocalTime.MIDNIGHT);
                break;
            case YEAR:
                truncLowerDateTime = lowerDateTime.with(TemporalAdjusters.firstDayOfYear()).with(LocalTime.MIDNIGHT);
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
            PartitionNameSetMap mvPartitionNameRefBaseTablePartitionMap,
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
                                "partition {}", mvPartitionNameRefBaseTablePartitionMap,
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
            PartitionNameSetMap mvPartitionNameRefBaseTablePartitionMap) {
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
        PartitionNameSetMap mvPartitionNameRefBaseTablePartitionMap = PartitionNameSetMap.of(mv.getRefreshScheme()
                        .getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap());
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

        PartitionNameSetMap mvPartitionNameRefBaseTablePartitionMap = PartitionNameSetMap.of(
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap());
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
                PCellSortedSet basePartitionMap = ((OlapTable) baseTable).getRangePartitionMap();
                PCellSortedSet mvPartitionRangeMap = PCellSortedSet.of();
                mvPartitionRangeMap.add(mvPartitionName, new PRangeCell(mvPartitionRange));
                PartitionNameSetMap mvToBaseMapping = getIntersectedPartitions(
                        mvPartitionRangeMap, basePartitionMap);
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

    /**
     * According base table and materialized view's partition range, we can define those mappings from base to mv:
     * <p>
     * One-to-One
     * src:     |----|    |----|
     * dst:     |----|    |----|
     * eg: base table is partitioned by one day, and mv is partition by one day
     * </p>
     * <p>
     * Many-to-One
     * src:     |----|    |----|     |----|    |----|
     * dst:     |--------------|     |--------------|
     * eg: base table is partitioned by one day, and mv is partition by date_trunc('month', dt)
     * <p>
     * One/Many-to-Many
     * src:     |----| |----| |----| |----| |----| |----|
     * dst:     |--------------| |--------------| |--------------|
     * eg: base table is partitioned by three days, and mv is partition by date_trunc('month', dt)
     * </p>
     * <p>
     * For one-to-one or many-to-one we can trigger to refresh by materialized view's partition, but for many-to-many
     * we need also consider affected materialized view partitions also.
     * <p>
     * eg:
     * ref table's partitions:
     * p0:   [2023-07-27, 2023-07-30)
     * p1:   [2023-07-30, 2023-08-02)
     * p2:   [2023-08-02, 2023-08-05)
     * materialized view's partition:
     * p0:   [2023-07-01, 2023-08-01)
     * p1:   [2023-08-01, 2023-09-01)
     * p2:   [2023-09-01, 2023-10-01)
     * <p>
     * So ref table's p1 has been changed, materialized view to refresh partition: p0, p1. And when we refresh p0,p1
     * we also need to consider other ref table partitions(p0); otherwise, the mv's final result will lose data.
     */
    public static boolean isCalcPotentialRefreshPartition(Map<Table, PCellSortedSet> baseChangedPartitionNames,
                                                          PCellSortedSet mvPartitions) {
        List<PRangeCell> mvSortedPartitionRanges = mvPartitions.getPartitions()
                .stream()
                .map(p -> (PRangeCell) p.cell())
                .collect(Collectors.toList());
        for (PCellSortedSet baseTableSortedSet : baseChangedPartitionNames.values()) {
            for (PCellWithName basePartitionRange : baseTableSortedSet.getPartitions()) {
                PRangeCell pRangeCell = (PRangeCell) basePartitionRange.cell();
                if (isManyToManyPartitionRangeMapping(pRangeCell, mvSortedPartitionRanges)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether srcRange is intersected with many dest ranges.
     */
    public static boolean isManyToManyPartitionRangeMapping(PRangeCell srcRange,
                                                            List<PRangeCell> dstRanges) {
        if (dstRanges.isEmpty()) {
            return false;
        }
        // quickly check if dst ranges only contain one range
        if (dstRanges.size() == 1) {
            return srcRange.isUnAligned(dstRanges.get(0));
        }
        List<PartitionKey> lowerPoints = dstRanges.stream().map(
                dstRange -> dstRange.getRange().lowerEndpoint()).toList();
        List<PartitionKey> upperPoints = dstRanges.stream().map(
                dstRange -> dstRange.getRange().upperEndpoint()).toList();

        PartitionKey lower = srcRange.getRange().lowerEndpoint();
        PartitionKey upper = srcRange.getRange().upperEndpoint();

        // For an interval [l, r], if there exists another interval [li, ri] that intersects with it, this interval
        // must satisfy l ≤ ri and r ≥ li. Therefore, if there exists a pos_a such that for all k < pos_a,
        // ri[k] < l, and there exists a pos_b such that for all k > pos_b, li[k] > r, then all intervals between
        // pos_a and pos_b might potentially intersect with the interval [l, r].
        int posA = PartitionKey.findLastLessEqualInOrderedList(lower, upperPoints);
        int posB = PartitionKey.findLastLessEqualInOrderedList(upper, lowerPoints);

        for (int i = posA; i <= posB; ++i) {
            if (dstRanges.get(i).isUnAligned(srcRange)) {
                return true;
            }
        }
        return false;
    }
}
