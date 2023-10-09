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

import com.google.common.annotations.VisibleForTesting;
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
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
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
import org.apache.commons.collections4.ListUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public static RangePartitionDiff getRangePartitionDiffOfSlotRef(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                    Map<String, Range<PartitionKey>> mvRangeMap) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        Map<String, Range<PartitionKey>> adds = diffRange(baseRangeMap, mvRangeMap);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, baseRangeMap);
        return new RangePartitionDiff(adds, deletes);
    }

    public static ListPartitionDiff getListPartitionDiff(Map<String, List<List<String>>> baseListMap,
                                                         Map<String, List<List<String>>> mvListMap) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        Map<String, List<List<String>>> adds = diffList(baseListMap, mvListMap);
        Map<String, List<List<String>>> deletes = diffList(mvListMap, baseListMap);
        return new ListPartitionDiff(adds, deletes);
    }

    public static boolean hasRangePartitionChanged(Map<String, Range<PartitionKey>> baseRangeMap,
                                                   Map<String, Range<PartitionKey>> mvRangeMap) {
        Map<String, Range<PartitionKey>> adds = diffRange(baseRangeMap, mvRangeMap);
        if (adds != null && !adds.isEmpty()) {
            return true;
        }
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRangeMap, baseRangeMap);
        return deletes != null && !deletes.isEmpty();
    }

    public static boolean hasListPartitionChanged(Map<String, List<List<String>>> baseRangeMap,
                                                  Map<String, List<List<String>>> mvRangeMap) {
        Map<String, List<List<String>>> adds = diffList(baseRangeMap, mvRangeMap);
        if (adds != null && !adds.isEmpty()) {
            return true;
        }
        Map<String, List<List<String>>> deletes = diffList(mvRangeMap, baseRangeMap);
        return deletes != null && !deletes.isEmpty();
    }

    public static RangePartitionDiff getRangePartitionDiffOfExpr(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                 Map<String, Range<PartitionKey>> mvRangeMap,
                                                                 FunctionCallExpr functionCallExpr,
                                                                 Range<PartitionKey> rangeToInclude) {
        PrimitiveType partitionType = functionCallExpr.getType().getPrimitiveType();
        Map<String, Range<PartitionKey>> rollupRange =
                mappingRangeList(baseRangeMap, partitionType, functionCallExpr);
        return getRangePartitionDiff(baseRangeMap, mvRangeMap, rollupRange, rangeToInclude);
    }

    @VisibleForTesting
    public static RangePartitionDiff getRangePartitionDiffOfExpr(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                 Map<String, Range<PartitionKey>> mvRangeMap,
                                                                 PrimitiveType partitionType) {
        Map<String, Range<PartitionKey>> rollupRange = mappingRangeList(baseRangeMap, partitionType, null);
        return getRangePartitionDiff(baseRangeMap, mvRangeMap, rollupRange, null);
    }

    @NotNull
    private static RangePartitionDiff getRangePartitionDiff(Map<String, Range<PartitionKey>> baseRangeMap,
                                                            Map<String, Range<PartitionKey>> mvRangeMap,
                                                            Map<String, Range<PartitionKey>> rollupRange,
                                                            Range<PartitionKey> rangeToInclude) {
        // TODO: Callers may use `List<PartitionRange>` directly.
        List<PartitionRange> rollupRanges = rollupRange.keySet().stream()
                .map(name -> new PartitionRange(name, rollupRange.get(name)))
                .collect(Collectors.toList());
        List<PartitionRange> baseRanges = baseRangeMap.keySet().stream()
                .map(name -> new PartitionRange(name, convertToDatePartitionRange(baseRangeMap.get(name))))
                .collect(Collectors.toList());
        List<PartitionRange> mvRanges = mvRangeMap.keySet().stream()
                .map(name -> new PartitionRange(name, mvRangeMap.get(name)))
                .collect(Collectors.toList());
        Map<String, Set<String>> partitionRefMap = getIntersectedPartitions(rollupRanges, baseRanges);
        Map<String, Range<PartitionKey>> adds = diffRange(rollupRanges, mvRanges, rangeToInclude);
        Map<String, Range<PartitionKey>> deletes = diffRange(mvRanges, rollupRanges, null);

        RangePartitionDiff diff = new RangePartitionDiff(adds, deletes);
        diff.setRollupToBasePartitionMap(partitionRefMap);
        return diff;
    }

    public static Map<String, Range<PartitionKey>> mappingRangeList(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                    PrimitiveType partitionType,
                                                                    FunctionCallExpr functionCallExpr) {
        Set<LocalDateTime> timePointSet = Sets.newTreeSet();
        for (Map.Entry<String, Range<PartitionKey>> rangeEntry : baseRangeMap.entrySet()) {
            PartitionMapping mappedRange = mappingRange(rangeEntry.getValue(), functionCallExpr);
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
                LocalDateTime upperDateTime = timePointList.get(i);
                PartitionKey upperPartitionKey = new PartitionKey();
                Type columnType = Type.fromPrimitiveType(partitionType);
                lowerPartitionKey.pushColumn(new DateLiteral(lowerDateTime, columnType), partitionType);
                upperPartitionKey.pushColumn(new DateLiteral(upperDateTime, columnType), partitionType);
                String mvPartitionName = getMVPartitionName(lowerDateTime, upperDateTime);
                result.put(mvPartitionName, Range.closedOpen(lowerPartitionKey, upperPartitionKey));
            } catch (AnalysisException ex) {
                throw new SemanticException("Convert to DateLiteral failed:", ex);
            }
        }
        return result;
    }

    private static PartitionRange convertToDatePartitionRange(PartitionRange range) {
        return new PartitionRange(range.getPartitionName(), convertToDatePartitionRange(range.getPartitionKeyRange()));
    }

    private static Range<PartitionKey> convertToDatePartitionRange(Range<PartitionKey> range) {
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
     * Evaluate the partition expression and partition boundary value.
     * E.g. PARTITION BY date_trunc('month', dt), and the value is '2023-08-21'
     * The evaluation is like: result = date_trunc('month', '2023-08-21')
     * <p>
     * <p>
     * NOTE: the current implementation does not support multiple function calls, like
     * `partition by date_trunc('month', str2date(str, '%Y-%m-%d'))`
     *
     * @param functionCallExpr PARTITION BY expression
     * @param value            partition boudary value
     * @return evaluate result
     */
    private static LocalDateTime evaluatePartitionExpr(FunctionCallExpr functionCallExpr, LiteralExpr value) {
        if (value instanceof MaxLiteral) {
            return new DateLiteral(Type.DATE, true).toLocalDateTime();
        }

        // translate the original PARTITION BY expr to a function call
        Type[] argTypes = functionCallExpr.getFn().getArgs();
        List<ScalarOperator> arguments = functionCallExpr.getParams().exprs().stream()
                .map(x -> x instanceof LiteralExpr ? x : value)
                .map(SqlToScalarOperatorTranslator::translate)
                .collect(Collectors.toList());

        String fnName = functionCallExpr.getFnName().getFunction();
        com.starrocks.catalog.Function fn =
                Expr.getBuiltinFunction(functionCallExpr.getFnName().getFunction(), argTypes,
                        com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkNotNull(fn, "function not found: " + fnName);

        CallOperator callOperator = new CallOperator(fnName, functionCallExpr.getType(), arguments, fn);

        ConstantOperator lowerResult = (ConstantOperator) ScalarOperatorEvaluator.INSTANCE.evaluation(callOperator);
        Preconditions.checkNotNull(lowerResult, "evaluate partition function failed: " + fnName);
        Preconditions.checkNotNull(lowerResult.getDate(), "evaluate result must be date/datetime time");

        return lowerResult.getDate();
    }

    public static PartitionMapping mappingRange(Range<PartitionKey> baseRange, FunctionCallExpr functionCallExpr) {
        // assume expr partition must be DateLiteral and only one partition
        baseRange = convertToDatePartitionRange(baseRange);
        LiteralExpr lowerExpr = baseRange.lowerEndpoint().getKeys().get(0);
        LiteralExpr upperExpr = baseRange.upperEndpoint().getKeys().get(0);
        return new PartitionMapping(evaluatePartitionExpr(functionCallExpr, lowerExpr),
                evaluatePartitionExpr(functionCallExpr, upperExpr));
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
        List<PartitionRange> srcRanges = srcRangeMap.keySet().stream()
                .map(name -> new PartitionRange(name, convertToDatePartitionRange(srcRangeMap.get(name))))
                .collect(Collectors.toList());
        List<PartitionRange> dstRanges = dstRangeMap.keySet().stream()
                .map(name -> new PartitionRange(name, convertToDatePartitionRange(dstRangeMap.get(name))))
                .collect(Collectors.toList());
        return getIntersectedPartitions(srcRanges, dstRanges);
    }

    /**
     * @param srcRanges : src partition ranges
     * @param dstRanges : dst partition ranges
     * @return : return all src partition name to intersected dst partition names which the src partition
     * is intersected with dst ranges.
     */
    public static Map<String, Set<String>> getIntersectedPartitions(List<PartitionRange> srcRanges,
                                                                    List<PartitionRange> dstRanges) {
        if (!srcRanges.isEmpty() && !dstRanges.isEmpty()) {
            List<PrimitiveType> srcTypes = srcRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            List<PrimitiveType> dstTypes = dstRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            Preconditions.checkArgument(Objects.equals(srcTypes, dstTypes), "types must be identical");
        }

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

    public static String getMVPartitionName(LocalDateTime lowerDateTime, LocalDateTime upperDateTime) {
        String lowerStr = lowerDateTime.format(DateUtils.MINUTE_FORMATTER_UNIX);
        lowerStr = StringUtils.removeEnd(lowerStr, "0");
        String upperStr = upperDateTime.format(DateUtils.MINUTE_FORMATTER_UNIX);
        upperStr = StringUtils.removeEnd(upperStr, "0");
        return lowerStr + "_" + upperStr;
    }

    public static Map<String, Range<PartitionKey>> diffRange(Map<String, Range<PartitionKey>> srcRangeMap,
                                                             Map<String, Range<PartitionKey>> dstRangeMap) {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, Range<PartitionKey>> srcEntry : srcRangeMap.entrySet()) {
            if (!dstRangeMap.containsKey(srcEntry.getKey()) ||
                    !RangeUtils.isRangeEqual(srcEntry.getValue(), dstRangeMap.get(srcEntry.getKey()))) {
                result.put(srcEntry.getKey(), convertToDatePartitionRange(srcEntry.getValue()));
            }
        }
        return result;
    }

    public static Map<String, Range<PartitionKey>> diffRange(List<PartitionRange> srcRanges,
                                                             List<PartitionRange> dstRanges,
                                                             Range<PartitionKey> rangeToInclude) {
        if (!srcRanges.isEmpty() && !dstRanges.isEmpty()) {
            List<PrimitiveType> srcTypes = srcRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            List<PrimitiveType> dstTypes = dstRanges.get(0).getPartitionKeyRange().lowerEndpoint().getTypes();
            Preconditions.checkArgument(Objects.equals(srcTypes, dstTypes), "types must be identical");
        }
        List<PartitionRange> diffs = ListUtils.subtract(srcRanges, dstRanges);
        return diffs.stream()
                .filter(r -> isRangeIncluded(r, rangeToInclude))
                .collect(Collectors.toMap(PartitionRange::getPartitionName,
                        diff -> convertToDatePartitionRange(diff).getPartitionKeyRange()
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
    private static boolean isRangeIncluded(PartitionRange range, Range<PartitionKey> rangeToInclude) {
        if (rangeToInclude == null) {
            return true;
        }
        Range<PartitionKey> rangeToCheck = range.getPartitionKeyRange();
        int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
        int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
        return !(lowerCmp >= 0 || upperCmp <= 0);
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
            Map<String, Range<PartitionKey>> rangeMap = materializedView.getValidRangePartitionMap(partitionTTLNumber);
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
            lastPartitionNum = Math.min(partitionTTLNumber, autoRefreshPartitionsLimit);
        } else if (isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = autoRefreshPartitionsLimit;
        } else if (partitionTTLNumber > 0) {
            lastPartitionNum = partitionTTLNumber;
        } else {
            lastPartitionNum = TableProperty.INVALID;
        }

        return materializedView.getValidRangePartitionMap(lastPartitionNum).keySet();
    }

    public static Set<String> getPartitionNamesByListWithPartitionLimit(MaterializedView materializedView,
                                                                        String start, String end,
                                                                        int partitionTTLNumber,
                                                                        boolean isAutoRefresh) {
        int autoRefreshPartitionsLimit = materializedView.getTableProperty().getAutoRefreshPartitionsLimit();
        boolean hasPartitionRange = StringUtils.isNoneEmpty(start) || StringUtils.isNoneEmpty(end);

        if (hasPartitionRange) {
            Set<String> result = Sets.newHashSet();

            Map<String, List<List<String>>> listMap = materializedView.getValidListPartitionMap(partitionTTLNumber);
            for (Map.Entry<String, List<List<String>>> entry : listMap.entrySet()) {
                if (entry.getKey().compareTo(start) >= 0 && entry.getKey().compareTo(end) <= 0) {
                    result.add(entry.getKey());
                }
            }
            return result;
        }

        int lastPartitionNum;
        if (partitionTTLNumber > 0 && isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = Math.min(partitionTTLNumber, autoRefreshPartitionsLimit);
            ;
        } else if (isAutoRefresh && autoRefreshPartitionsLimit > 0) {
            lastPartitionNum = autoRefreshPartitionsLimit;
        } else if (partitionTTLNumber > 0) {
            lastPartitionNum = partitionTTLNumber;
        } else {
            lastPartitionNum = TableProperty.INVALID;
        }

        return materializedView.getValidListPartitionMap(lastPartitionNum).keySet();
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
                Map<String, Set<String>> mvToBaseMapping = getIntersectedPartitions(
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
                    baseTable.getName(), baseTable.getTableIdentifier());
            Map<String, MaterializedView.BasePartitionInfo> baseTableVersionMap = versionMap.get(baseTableInfo);
            if (baseTableVersionMap != null) {
                baseTableVersionMap.keySet().removeIf(partitionName -> {
                    try {
                        boolean isListPartition = mv.getPartitionInfo() instanceof ListPartitionInfo;
                        Set<String> partitionNames = PartitionUtil.getMVPartitionName(baseTable, partitionColumn,
                                Lists.newArrayList(partitionName), isListPartition);
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
                    baseTable.getName(), baseTable.getTableIdentifier()));
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
