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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public static PartitionDiff calcSyncSamePartition(Map<String, Range<PartitionKey>> baseRangeMap,
                                                      Map<String, Range<PartitionKey>> mvRangeMap) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        Map<String, Range<PartitionKey>> adds = diffRange(baseRangeMap, mvRangeMap);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, baseRangeMap);
        return new PartitionDiff(adds, deletes);
    }

    public static boolean hasPartitionChange(Map<String, Range<PartitionKey>> baseRangeMap,
                                             Map<String, Range<PartitionKey>> mvRangeMap) {
        Map<String, Range<PartitionKey>> adds = diffRange(baseRangeMap, mvRangeMap);
        if (adds != null && !adds.isEmpty()) {
            return true;
        }
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, baseRangeMap);
        return deletes != null && !deletes.isEmpty();
    }

    public static PartitionDiff calcSyncRollupPartition(Map<String, Range<PartitionKey>> baseRangeMap,
                                                        Map<String, Range<PartitionKey>> mvRangeMap,
                                                        String granularity, PrimitiveType partitionType) {
        Map<String, Range<PartitionKey>> rollupRange = mappingRangeList(baseRangeMap, granularity, partitionType);
<<<<<<< HEAD
        Map<String, Set<String>> partitionRefMap = generatePartitionRefMap(rollupRange, baseRangeMap);
        Map<String, Range<PartitionKey>> adds = diffRange(rollupRange, mvRangeMap);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, rollupRange);
        PartitionDiff diff = new PartitionDiff(adds, deletes);
=======

        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PartitionRange> rollupRanges = rollupRange.keySet().stream().map(name -> new PartitionRange(name,
                rollupRange.get(name))).collect(Collectors.toList());
        List<PartitionRange> baseRanges = baseRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                baseRangeMap.get(name))).collect(Collectors.toList());
        List<PartitionRange> mvRanges = mvRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                mvRangeMap.get(name))).collect(Collectors.toList());
        Map<String, Set<String>> partitionRefMap = generatePartitionRefMap(rollupRanges, baseRanges);
        Map<String, Range<PartitionKey>> adds = diffRange(rollupRanges, mvRanges);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRanges, rollupRanges);

        RangePartitionDiff diff = new RangePartitionDiff(adds, deletes);
>>>>>>> 901cd027a ([Enhancement] Optimize generatePartitionRefMap performance when there are many partitions in base table (#26824))
        diff.setRollupToBasePartitionMap(partitionRefMap);
        return diff;

    }

    public static Map<String, Range<PartitionKey>> mappingRangeList(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                    String granularity, PrimitiveType partitionType) {
        Set<LocalDateTime> timePointSet = Sets.newTreeSet();
        try {
            for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
                PartitionMapping mappedRange = mappingRange(rangeEntry.getValue(), granularity);
                // this mappedRange may exist range overlap
                timePointSet.add(mappedRange.getLowerDateTime());
                timePointSet.add(mappedRange.getUpperDateTime());
            }
        } catch (AnalysisException e) {
            throw new SemanticException("Convert to PartitionMapping failed:", e);
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
                LocalDateTime upperDateTime = timePointList.get(i);
                PartitionKey upperPartitionKey = new PartitionKey();
                if (partitionType == PrimitiveType.DATE) {
                    lowerPartitionKey.pushColumn(new DateLiteral(lowerDateTime, Type.DATE), partitionType);
                    upperPartitionKey.pushColumn(new DateLiteral(upperDateTime, Type.DATE), partitionType);
                } else {
                    lowerPartitionKey.pushColumn(new DateLiteral(lowerDateTime, Type.DATETIME), partitionType);
                    upperPartitionKey.pushColumn(new DateLiteral(upperDateTime, Type.DATETIME), partitionType);
                }
                String mvPartitionName = getMVPartitionName(lowerDateTime, upperDateTime, granularity);
                result.put(mvPartitionName, Range.closedOpen(lowerPartitionKey, upperPartitionKey));
            } catch (AnalysisException ex) {
                throw new SemanticException("Convert to DateLiteral failed:", ex);
            }
        }
        return result;
    }

    public static PartitionMapping mappingRange(Range<PartitionKey> baseRange, String granularity)
            throws AnalysisException {
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
        return new PartitionMapping(truncLowerDateTime, truncUpperDateTime);
    }

    public static Map<String, Set<String>> generatePartitionRefMap(Map<String, Range<PartitionKey>> srcRangeMap,
                                                                   Map<String, Range<PartitionKey>> dstRangeMap) {
        if (dstRangeMap.isEmpty()) {
            return srcRangeMap.keySet().stream().collect(Collectors.toMap(Function.identity(), Sets::newHashSet));
        }

        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PartitionRange> srcRanges = srcRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                srcRangeMap.get(name))).collect(Collectors.toList());
        List<PartitionRange> dstRanges = dstRangeMap.keySet().stream().map(name -> new PartitionRange(name,
                dstRangeMap.get(name))).collect(Collectors.toList());
        return generatePartitionRefMap(srcRanges, dstRanges);
    }

    private static Map<String, Set<String>> generatePartitionRefMap(List<PartitionRange> srcRanges,
                                                                    List<PartitionRange> dstRanges) {
        Map<String, Set<String>> result = srcRanges.stream().collect(
                Collectors.toMap(PartitionRange::getPartitionName, x -> Sets.newHashSet()));

        Collections.sort(srcRanges, PartitionRange::compareTo);
        Collections.sort(dstRanges, PartitionRange::compareTo);

        for (PartitionRange srcRange : srcRanges) {
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
                                                     Set<String> baseChangedPartitionNames,
                                                     Map<String, Set<String>> baseToMvNameRef,
                                                     Map<String, Set<String>> mvToBaseNameRef) {
        gatherPotentialRefreshPartitionNames(needRefreshMvPartitionNames, baseChangedPartitionNames,
                baseToMvNameRef, mvToBaseNameRef);
    }

    private static void gatherPotentialRefreshPartitionNames(Set<String> needRefreshMvPartitionNames,
                                                             Set<String> baseChangedPartitionNames,
                                                             Map<String, Set<String>> baseToMvNameRef,
                                                             Map<String, Set<String>> mvToBaseNameRef) {
        int curNameCount = needRefreshMvPartitionNames.size();
        Set<String> newBaseChangedPartitionNames = Sets.newHashSet();
        Set<String> newNeedRefreshMvPartitionNames = Sets.newHashSet();
        for (String needRefreshMvPartitionName : needRefreshMvPartitionNames) {
            Set<String> baseNames = mvToBaseNameRef.get(needRefreshMvPartitionName);
            newBaseChangedPartitionNames.addAll(baseNames);
            for (String baseName : baseNames) {
                Set<String> mvNames = baseToMvNameRef.get(baseName);
                newNeedRefreshMvPartitionNames.addAll(mvNames);
            }
        }
        baseChangedPartitionNames.addAll(newBaseChangedPartitionNames);
        needRefreshMvPartitionNames.addAll(newNeedRefreshMvPartitionNames);
        if (curNameCount != needRefreshMvPartitionNames.size()) {
            gatherPotentialRefreshPartitionNames(needRefreshMvPartitionNames, baseChangedPartitionNames,
                    baseToMvNameRef, mvToBaseNameRef);
        }
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
    private static LocalDateTime getUpperDateTime(LocalDateTime upperDateTime, String granularity) {
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
        return truncUpperDateTime;
    }

    private static LocalDateTime getLowerDateTime(LocalDateTime lowerDateTime, String granularity) {
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

    public static Map<String, Range<PartitionKey>> diffRange(Map<String, Range<PartitionKey>> srcRangeMap,
                                                             Map<String, Range<PartitionKey>> dstRangeMap) {

        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeMap.entrySet()) {
            if (!dstRangeMap.containsKey(srcEntry.getKey()) ||
                    !RangeUtils.isRangeEqual(srcEntry.getValue(), dstRangeMap.get(srcEntry.getKey()))) {
                result.put(srcEntry.getKey(), srcEntry.getValue());
            }
        }
        return result;
    }

<<<<<<< HEAD
=======
    public static Map<String, Range<PartitionKey>> diffRange(List<PartitionRange> srcRanges,
                                                             List<PartitionRange> dstRanges) {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        Set<PartitionRange> dstRangeSet = dstRanges.stream().collect(Collectors.toSet());
        for (PartitionRange range : srcRanges) {
            if (!dstRangeSet.contains(range)) {
                result.put(range.getPartitionName(), range.getPartitionKeyRange());
            }
        }
        return result;
    }

    public static Map<String, List<List<String>>> diffList(Map<String, List<List<String>>> srcListMap,
                                                             Map<String, List<List<String>>> dstListMap) {

        Map<String, List<List<String>>> result = Maps.newHashMap();
        for (Map.Entry<String, List<List<String>>> srcEntry : srcListMap.entrySet()) {
            String key = srcEntry.getKey();
            if (!dstListMap.containsKey(key) ||
                    ListPartitionInfo.compareByValue(srcListMap.get(key), dstListMap.get(key)) != 0) {
                result.put(key, srcEntry.getValue());
            }
        }
        return result;
    }

>>>>>>> 901cd027a ([Enhancement] Optimize generatePartitionRefMap performance when there are many partitions in base table (#26824))
    public static Set<String> getPartitionNamesByRangeWithPartitionLimit(MaterializedView materializedView,
                                                                         String start, String end,
                                                                         int partitionTTLNumber,
                                                                         boolean isAutoRefresh)
            throws AnalysisException {
        int autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
        boolean hasPartitionRange = StringUtils.isNoneEmpty(start) || StringUtils.isNoneEmpty(end);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();
            Column partitionColumn =
                    ((RangePartitionInfo) materializedView.getPartitionInfo()).getPartitionColumns().get(0);
            Range<PartitionKey> rangeToInclude = createRange(start, end, partitionColumn);
            Map<String, Range<PartitionKey>> rangeMap = materializedView.getValidPartitionMap(partitionTTLNumber);
            for (Map.Entry<String, Range<PartitionKey>> entry : rangeMap.entrySet()) {
                Range<PartitionKey> rangeToCheck = entry.getValue();
                int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
                int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
                if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        int lastPartitionNum;
        if (partitionTTLNumber > 0 && isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = Math.min(partitionTTLNumber, autoRefreshPartitionsLimit);;
        } else if (isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = autoRefreshPartitionsLimit;
        } else if (partitionTTLNumber > 0)  {
            lastPartitionNum = partitionTTLNumber;
        } else {
            lastPartitionNum = TableProperty.INVALID;
        }

        return materializedView.getValidPartitionMap(lastPartitionNum).keySet();
    }

    public static Range<PartitionKey> createRange(String lowerBound, String upperBound, Column partitionColumn)
            throws AnalysisException {
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

        Database baseDb = GlobalStateMgr.getCurrentState().getDb(tableName.getDb());
        if (baseDb == null) {
            return;
        }
        Table baseTable = baseDb.getTable(tableName.getTbl());
        if (baseTable == null) {
            return;
        }
        long tableId = baseTable.getId();
        if (expr instanceof SlotRef) {
            Map<String, MaterializedView.BasePartitionInfo> mvTableVersionMap = versionMap.get(tableId);
            // mv partition name same as base table.
            if (mvTableVersionMap != null) {
                mvTableVersionMap.remove(mvPartitionName);
            }
        } else if (expr instanceof FunctionCallExpr) {
            Map<String, MaterializedView.BasePartitionInfo> mvTableVersionMap = versionMap.get(tableId);
            if (mvTableVersionMap != null && mvPartitionRange != null && baseTable instanceof OlapTable) {
                // use range derive connect base partition
                Map<String, Range<PartitionKey>> basePartitionMap = ((OlapTable) baseTable).getRangePartitionMap();
                Map<String, Set<String>> mvToBaseMapping = generatePartitionRefMap(
                        Collections.singletonMap(mvPartitionName, mvPartitionRange), basePartitionMap);
                mvToBaseMapping.values().forEach(parts -> parts.forEach(mvTableVersionMap::remove));
            }
        } else {
            // This is a bad case for refreshing, and this problem will be optimized later.
            versionMap.remove(tableId);
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
        Expr expr = mv.getPartitionRefTableExprs().get(0);
        Table baseTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl());

        if (baseTable == null) {
            return;
        }
        if (expr instanceof SlotRef) {
            Column partitionColumn = baseTable.getColumn(((SlotRef) expr).getColumnName());
            BaseTableInfo baseTableInfo = new BaseTableInfo(tableName.getCatalog(), tableName.getDb(),
                    baseTable.getTableIdentifier());
            Map<String, MaterializedView.BasePartitionInfo> baseTableVersionMap = versionMap.get(baseTableInfo);
            if (baseTableVersionMap != null) {
                baseTableVersionMap.keySet().removeIf(partitionName -> {
                    try {
                        Set<String> partitionNames = PartitionUtil.getMVPartitionName(baseTable, partitionColumn,
                                Lists.newArrayList(partitionName));
                        return partitionNames != null && partitionNames.size() == 1 &&
                                Lists.newArrayList(partitionNames).get(0).equals(mvPartitionName);
                    } catch (AnalysisException e) {
                        LOG.warn("failed to get mv partition name", e);
                        return false;
                    }
                });
            }
        } else {
            // This is a bad case for refreshing, and this problem will be optimized later.
            versionMap.remove(new BaseTableInfo(tableName.getCatalog(), tableName.getDb(),
                    baseTable.getTableIdentifier()));
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
