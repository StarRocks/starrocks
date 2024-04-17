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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import org.apache.commons.lang3.tuple.Triple;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

/*
 *         agg                            new_agg
 *          ｜                               |
 *        filter         =>               union
 *          ｜                        /      |      \
 *        input                     agg1   agg2    ...
 *                                   |       |      |
 *                                filter1 filter2  ...
 *
 * For query like select sum(k2) from tbl where  k1 >= "2022-01-02" and  k1 <= "2024-05-06" group by k3
 * The rule will change the query like :
 * select sum(k2) from
 * (
 * select sum(k2) as k2 from tbl where k1 >= "2022-01-02" and k1 < "2022-02-01" group by k3
 * union all
 * select sum(k2) as k2 from tbl where date_trunc('month', k1) >= "2022-02-01"
 * and date_trunc('month', k1) < "2023-01-01" group by k3
 * union all
 * select sum(k2) as k2 from tbl where date_trunc('year', k1) >= "2023-01-01"
 * and date_trunc('year', k1) < "2024-01-01" group by k3
 * union all
 * select sum(k2) as k2 from tbl where date_trunc('month', k1) >= "2024-01-01"
 * and date_trunc('month', k1) < "2024-05-01" group by k3
 * union all
 * select sum(k2) as k2 from tbl where k1 >= "2024-05-01" and k1 <= "2024-05-06" group by k3
 * ) t
 * group by k3
 * The transformed query is easy to be rewritten by MV on day/month/year dimension.
 */

public class FineGrainedRangePredicateRule extends TransformationRule {

    public static final FineGrainedRangePredicateRule INSTANCE = new FineGrainedRangePredicateRule(
            RuleType.TF_FINE_GRAINED_RANGE_PREDICATE,
            Pattern.create(OperatorType.LOGICAL_AGGR)
                    .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER)
                            .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))));

    public static final FineGrainedRangePredicateRule PROJECTION_INSTANCE = new FineGrainedRangePredicateRule(
            RuleType.TF_FINE_GRAINED_RANGE_PREDICATE,
            Pattern.create(OperatorType.LOGICAL_AGGR)
                    .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)
                            .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER)
                                    .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)))));

    // the supported agg functions should use the middle result from child input to calculate the final result.
    // Except count/sum, the other agg functions use the same type between their argument and their return result.
    private static final Set<String> SUPPORTED_FUNC = ImmutableSet.of(FunctionSet.COUNT, FunctionSet.SUM, FunctionSet.MIN,
            FunctionSet.MAX, FunctionSet.HLL_UNION, FunctionSet.BITMAP_UNION, FunctionSet.PERCENTILE_UNION);

    public FineGrainedRangePredicateRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!MvUtils.isLogicalSPJG(input)) {
            return false;
        }

        LogicalAggregationOperator aggOp = input.getOp().cast();
        if (aggOp.getPredicate() != null) {
            return false;
        }

        // only support count, sum, max, min agg functions.
        for (CallOperator callOperator : aggOp.getAggregations().values()) {
            if (callOperator.isDistinct()) {
                return false;
            }

            if (!SUPPORTED_FUNC.contains(callOperator.getFnName())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator;
        boolean withProjection = false;
        if (input.inputAt(0).getOp().getOpType() == OperatorType.LOGICAL_PROJECT) {
            withProjection = true;
            filterOperator = input.inputAt(0).inputAt(0).getOp().cast();
        } else {
            filterOperator = input.inputAt(0).getOp().cast();
        }

        List<ScalarOperator> otherPredicates = Lists.newArrayList();
        Map<ColumnRefOperator, List<BinaryPredicateOperator>> columnToRange = Maps.newHashMap();
        extractRangePredicate(filterOperator.getPredicate(), otherPredicates, columnToRange);
        if (columnToRange.isEmpty()) {
            return Lists.newArrayList();
        }

        // only handle ranges like [,] or (,] or [,) or (,)
        Iterator<Map.Entry<ColumnRefOperator, List<BinaryPredicateOperator>>> iterator =
                columnToRange.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ColumnRefOperator, List<BinaryPredicateOperator>> entry = iterator.next();
            List<BinaryPredicateOperator> colPredicates = entry.getValue();

            long leLtNum = colPredicates.stream()
                    .map(BinaryPredicateOperator::getBinaryType)
                    .filter(t -> BinaryType.LE.equals(t) || BinaryType.LT.equals(t))
                    .count();

            long geGtNum = colPredicates.stream()
                    .map(BinaryPredicateOperator::getBinaryType)
                    .filter(t -> BinaryType.GE.equals(t) || BinaryType.GT.equals(t))
                    .count();

            if (leLtNum != 1 || geGtNum != 1) {
                otherPredicates.addAll(entry.getValue()); // Add to the list before removing
                iterator.remove(); // Remove the current element from the map
            }
        }

        if (columnToRange.size() != 1) {
            return Lists.newArrayList();
        }

        List<ScalarOperator> ranges = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, List<BinaryPredicateOperator>> entry : columnToRange.entrySet()) {
            ranges = buildRangePredicates(entry.getKey(), entry.getValue());
        }
        if (ranges.size() <= 1) {
            return Lists.newArrayList();
        }

        List<ColumnOutputInfo> aggColInfoList = input.getRowOutputInfo().getColumnOutputInfo();
        List<ColumnRefOperator> unionOutputCols = aggColInfoList.stream().map(ColumnOutputInfo::getColumnRef)
                .collect(Collectors.toList());

        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        List<OptExpression> childrenOfUnion = Lists.newArrayList();

        for (ScalarOperator predicate : ranges) {
            OptExpression unionChild = buildOriginalUnionChildOpt(predicate, otherPredicates, input, withProjection);
            OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
            OptExpression newChildOpt = duplicator.duplicate(unionChild);
            List<ColumnRefOperator> orderedCols = Lists.newArrayList();
            for (ColumnOutputInfo colInfo : aggColInfoList) {
                orderedCols.add(duplicator.getColumnMapping().get(colInfo.getColumnRef()));
            }
            childOutputColumns.add(orderedCols);
            childrenOfUnion.add(newChildOpt);
        }
        OptExpression unionOpt = OptExpression.create(
                new LogicalUnionOperator(unionOutputCols, childOutputColumns, true),
                childrenOfUnion);

        LogicalAggregationOperator newAggOp = rewriteAggOperator(aggColInfoList, unionOutputCols);

        return Lists.newArrayList(OptExpression.create(newAggOp, unionOpt));
    }

    private OptExpression buildOriginalUnionChildOpt(ScalarOperator predicate, List<ScalarOperator> otherPredicates,
                                                     OptExpression input, boolean withProjection) {
        List<ScalarOperator> allPredicates = Lists.newArrayList();
        allPredicates.add(predicate);
        allPredicates.addAll(otherPredicates);
        LogicalFilterOperator filterOperator = new LogicalFilterOperator(Utils.compoundAnd(allPredicates));

        OptExpression result;
        if (withProjection) {
            OptExpression filterOpt = OptExpression.create(filterOperator, input.inputAt(0).inputAt(0).inputAt(0));
            OptExpression projectionOpt = OptExpression.create(input.inputAt(0).getOp(), filterOpt);
            result = OptExpression.create(input.getOp(), projectionOpt);
        } else {
            OptExpression filterOpt = OptExpression.create(filterOperator, input.inputAt(0).inputAt(0));
            result = OptExpression.create(input.getOp(), filterOpt);
        }

        return result;

    }

    private void extractRangePredicate(ScalarOperator predicate,
                                       List<ScalarOperator> resultPredicates,
                                       Map<ColumnRefOperator, List<BinaryPredicateOperator>> columnToRange) {
        List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
        for (ScalarOperator p : predicates) {
            if (!(p instanceof BinaryPredicateOperator)) {
                resultPredicates.add(p);
                continue;
            }
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) p;
            if (!binaryPredicate.getBinaryType().isRange()) {
                resultPredicates.add(p);
                continue;
            }

            if (!(binaryPredicate.getChild(0) instanceof ColumnRefOperator)) {
                resultPredicates.add(p);
                continue;
            }
            ColumnRefOperator child0 = (ColumnRefOperator) binaryPredicate.getChild(0);
            if (!child0.getType().isDate() && !child0.getType().isDatetime()) {
                resultPredicates.add(p);
                continue;
            }

            if (!(binaryPredicate.getChild(1) instanceof ConstantOperator)) {
                resultPredicates.add(p);
                continue;
            }
            ConstantOperator child1 = (ConstantOperator) binaryPredicate.getChild(1);
            if (!child1.getType().isDate() && !child1.getType().isDatetime()) {
                resultPredicates.add(p);
                continue;
            }

            ColumnRefOperator columnRefOperator = (ColumnRefOperator) binaryPredicate.getChild(0);

            columnToRange.computeIfAbsent(columnRefOperator, k -> Lists.newArrayList()).add(binaryPredicate);
        }
    }

    private LogicalAggregationOperator rewriteAggOperator(List<ColumnOutputInfo> aggColInfoList,
                                                          List<ColumnRefOperator> unionOutputCols) {
        List<ColumnRefOperator> newGroupByKeys = Lists.newArrayList();
        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        for (int i = 0; i < aggColInfoList.size(); i++) {
            ColumnOutputInfo info = aggColInfoList.get(i);
            ColumnRefOperator inputCol = unionOutputCols.get(i);
            if (info.getScalarOp().isColumnRef()) {
                newGroupByKeys.add(info.getColumnRef());
            } else {
                CallOperator callOperator = (CallOperator) info.getScalarOp();
                if (FunctionSet.SUM.equals(callOperator.getFnName()) || FunctionSet.COUNT.equals(callOperator.getFnName())) {
                    Type[] argTypes = new Type[] {inputCol.getType()};
                    Function newFunc = Expr.getBuiltinFunction(FunctionSet.SUM, argTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    Preconditions.checkNotNull(newFunc);
                    newFunc = newFunc.copy();
                    // for decimal
                    newFunc = newFunc.updateArgType(argTypes);
                    newFunc.setRetType(callOperator.getFunction().getReturnType());
                    CallOperator newAggCall = new CallOperator(FunctionSet.SUM, newFunc.getReturnType(),
                            Lists.newArrayList(inputCol), newFunc);
                    Preconditions.checkState(newAggCall.getType().equals(info.getColumnRef().getType()),
                            "the rewrite agg call return type %s should equals with the output col type '%'",
                            newAggCall.getType(), info.getColumnRef().getType());
                    newAggCalls.put(info.getColumnRef(), newAggCall);
                } else {
                    CallOperator newAggCall = new CallOperator(callOperator.getFnName(), callOperator.getType(),
                            Lists.newArrayList(inputCol), callOperator.getFunction());
                    newAggCalls.put(info.getColumnRef(), newAggCall);
                }

            }
        }
        return new LogicalAggregationOperator(AggType.GLOBAL, newGroupByKeys, newAggCalls);
    }

    @VisibleForTesting
    public static List<ScalarOperator> buildRangePredicates(ColumnRefOperator curColumn,
                                                            List<BinaryPredicateOperator> colPredicates) {
        BinaryPredicateOperator leftDayBeginPredicate = colPredicates.get(0);
        BinaryPredicateOperator rightDayEndPredicate = colPredicates.get(1);

        BinaryType firstType = leftDayBeginPredicate.getBinaryType();
        BinaryType secondType = rightDayEndPredicate.getBinaryType();

        //  make sure predicate is "col >= left And col <= right"
        if (BinaryType.LE.equals(firstType) || BinaryType.LT.equals(firstType)) {
            Collections.swap(colPredicates, 0, 1);
        }

        // calculate time points first
        ConstantOperator begin = (ConstantOperator) leftDayBeginPredicate.getChild(1);
        ConstantOperator end = (ConstantOperator) rightDayEndPredicate.getChild(1);
        boolean isDate = begin.getType().isDate();
        List<Triple<String, LocalDateTime, LocalDateTime>> ranges =
                cutDateRange(begin.getDatetime(), end.getDatetime());

        List<ScalarOperator> result = Lists.newArrayList();

        int idx = 0;
        for (Triple<String, LocalDateTime, LocalDateTime> range : ranges) {
            ScalarOperator predicate;
            LocalDateTime left = isDate ? range.getMiddle().truncatedTo(ChronoUnit.DAYS) : range.getMiddle();
            LocalDateTime right = isDate ? range.getRight().truncatedTo(ChronoUnit.DAYS) : range.getRight();
            ConstantOperator leftConstant = isDate ? ConstantOperator.createDate(left) :
                    ConstantOperator.createDatetime(left);
            ConstantOperator rightConstant = isDate ? ConstantOperator.createDate(right) :
                    ConstantOperator.createDatetime(right);
            if ("DAY".equals(range.getLeft())) {
                if (idx == 0 && ranges.size() == 1) {
                    BinaryPredicateOperator leftRange = new BinaryPredicateOperator(firstType, curColumn, leftConstant);
                    BinaryPredicateOperator rightRange =
                            new BinaryPredicateOperator(secondType, curColumn, rightConstant);
                    predicate = Utils.compoundAnd(leftRange, rightRange);
                } else if (idx == 0) {
                    BinaryPredicateOperator leftRange = new BinaryPredicateOperator(firstType, curColumn, leftConstant);
                    BinaryPredicateOperator rightRange =
                            new BinaryPredicateOperator(BinaryType.LT, curColumn, rightConstant);
                    predicate = Utils.compoundAnd(leftRange, rightRange);
                } else {
                    BinaryPredicateOperator leftRange =
                            new BinaryPredicateOperator(BinaryType.GE, curColumn, leftConstant);
                    BinaryPredicateOperator rightRange =
                            new BinaryPredicateOperator(secondType, curColumn, rightConstant);
                    predicate = Utils.compoundAnd(leftRange, rightRange);
                }
            } else if ("MONTH".equals(range.getLeft())) {
                CallOperator callOperator = buildDateTrunc(ConstantOperator.createVarchar("month"), curColumn);
                BinaryPredicateOperator leftRange =
                        new BinaryPredicateOperator(BinaryType.GE, callOperator, leftConstant);
                BinaryPredicateOperator rightRange =
                        new BinaryPredicateOperator(BinaryType.LT, callOperator, rightConstant);
                predicate = Utils.compoundAnd(leftRange, rightRange);
            } else {
                CallOperator callOperator = buildDateTrunc(ConstantOperator.createVarchar("year"), curColumn);
                BinaryPredicateOperator leftRange =
                        new BinaryPredicateOperator(BinaryType.GE, callOperator, leftConstant);
                BinaryPredicateOperator rightRange =
                        new BinaryPredicateOperator(BinaryType.LT, callOperator, rightConstant);
                predicate = Utils.compoundAnd(leftRange, rightRange);
            }
            result.add(predicate);
            idx++;
        }
        return result;
    }

    @VisibleForTesting
    public static List<Triple<String, LocalDateTime, LocalDateTime>> cutDateRange(LocalDateTime beginTime,
                                                                                  LocalDateTime endTime) {
        List<Triple<String, LocalDateTime, LocalDateTime>> result = Lists.newArrayList();
        LocalDateTime endMonthOfBegin = beginTime.with(TemporalAdjusters.lastDayOfMonth())
                .plusDays(1).truncatedTo(ChronoUnit.DAYS);
        LocalDateTime endYearOfBegin = beginTime.with(TemporalAdjusters.lastDayOfYear())
                .plusDays(1).truncatedTo(ChronoUnit.DAYS);
        LocalDateTime beginMonthOfEnd = endTime.with(TemporalAdjusters.firstDayOfMonth())
                .truncatedTo(ChronoUnit.DAYS);
        LocalDateTime beginYearOfEnd = endTime.with(TemporalAdjusters.firstDayOfYear())
                .truncatedTo(ChronoUnit.DAYS);

        if (beginTime.compareTo(endTime) > 0) {
            return result;
        }

        if (ChronoUnit.DAYS.between(endMonthOfBegin, endTime) < 31) {
            result.add(Triple.of("DAY", beginTime, endTime));
        } else if (ChronoUnit.YEARS.between(endYearOfBegin, beginYearOfEnd) < 1) {
            result.add(Triple.of("DAY", beginTime, endMonthOfBegin));
            result.add(Triple.of("MONTH", endMonthOfBegin, beginMonthOfEnd));
            result.add(Triple.of("DAY", beginMonthOfEnd, endTime));

        } else {
            result.add(Triple.of("DAY", beginTime, endMonthOfBegin));
            if (endMonthOfBegin.compareTo(endYearOfBegin) < 0) {
                result.add(Triple.of("MONTH", endMonthOfBegin, endYearOfBegin));
            }
            result.add(Triple.of("YEAR", endYearOfBegin, beginYearOfEnd));
            if (beginYearOfEnd.compareTo(beginMonthOfEnd) < 0) {
                result.add(Triple.of("MONTH", beginYearOfEnd, beginMonthOfEnd));
            }
            result.add(Triple.of("DAY", beginMonthOfEnd, endTime));
        }

        return result;
    }

    private static CallOperator buildDateTrunc(ScalarOperator arg1, ScalarOperator arg2) {
        Type type;
        if (arg2.getType().isDatetime()) {
            type = Type.DATETIME;
        } else {
            type = Type.DATE;
        }

        Function searchDesc = new Function(new FunctionName(FunctionSet.DATE_TRUNC),
                new Type[] {Type.VARCHAR, type}, type, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator result = new CallOperator(FunctionSet.DATE_TRUNC, type, Lists.newArrayList(arg1, arg2), fn);
        return result;
    }

}
