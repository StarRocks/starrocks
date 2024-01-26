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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

// For query like select sum() from xxx where  k1 >= "2022-01-02" and "2024-05-06" >= k1 group by xxx
// we can create mv in year/month/day dimension to pre-agg result
// this rule automatically change k1 >= "2022-01-02" and "2024-05-06" >= k1  into format like below:
// (((1: k1 >= '2022-01-02') AND (1: k1 < '2022-02-01'))
// OR ((1: k1 >= '2024-05-01') AND (1: k1 <= '2024-05-06')))
// OR (((date_trunc('month', 1: k1) >= '2022-02-01') AND (date_trunc('month', 1: k1) < '2023-01-01'))
// OR ((date_trunc('month', 1: k1) >= '2024-01-01') AND (date_trunc('month', 1: k1) < '2024-05-01'))))
// OR ((date_trunc('year', 1: k1) >= '2023-01-01') AND (date_trunc('year', 1: k1) < '2024-01-01'))
// and another rule "SplitScanORToUnionRule" can rewrite OR to Union, which can be rewritten by mv to speed up query
public class SplitDatePredicateRule extends TransformationRule {

    private static final Logger LOG = LogManager.getLogger(SplitDatePredicateRule.class);

    public SplitDatePredicateRule() {
        super(RuleType.TF_SPLIT_PREDICATE_WITH_DATE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    private static CallOperator buildDateTrunc(List<ScalarOperator> arguments) {
        ScalarOperator arg1 = arguments.get(1);
        Type type;
        if (arg1.getType().isDatetime()) {
            type = Type.DATETIME;
        } else {
            type = Type.DATE;
        }

        Function searchDesc = new Function(new FunctionName(FunctionSet.DATE_TRUNC),
                new Type[] {Type.VARCHAR, type}, type, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator result = new CallOperator(FunctionSet.DATE_TRUNC, type, arguments, fn);
        return result;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();
        return context.getSessionVariable().isCboSplitScanPredicateWithDate() && scan.getPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();

        List<ScalarOperator> predicates = Utils.extractConjuncts(scan.getPredicate());
        // store the final predicates
        List<ScalarOperator> resultPredicates = new ArrayList<>();
        Map<ColumnRefOperator, List<BinaryPredicateOperator>> columnToRange = Maps.newHashMap();
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

        // only handle [,]/(,]/[/)/[]
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
                resultPredicates.addAll(entry.getValue()); // Add to the list before removing
                iterator.remove(); // Remove the current element from the map
            }
        }

        if (columnToRange.isEmpty()) {
            return Lists.newArrayList(input);
        }

        // rewrite predicate
        for (Map.Entry<ColumnRefOperator, List<BinaryPredicateOperator>> entry : columnToRange.entrySet()) {
            //      leftDayPredicate           leftMonthPredicate                   yearPredicate
            // [left_day_begin,left_month_begin),[left_month_begin,year_begin),[year_begin,right_month_begin),
            //      rightMonthPredicate                 rightDayPredicate
            // [right_month_begin,right_day_begin),[right_day_begin,right_day_end]

            List<ScalarOperator> curResultPredicates = new ArrayList<>();
            ColumnRefOperator curColumn = entry.getKey();
            List<BinaryPredicateOperator> colPredicates = entry.getValue();

            BinaryPredicateOperator leftDayBeginPredicate = colPredicates.get(0);
            BinaryPredicateOperator rightDayEndPredicate = colPredicates.get(1);

            BinaryType firstPredicateType = leftDayBeginPredicate.getBinaryType();

            if (BinaryType.LE.equals(firstPredicateType) || BinaryType.LT.equals(firstPredicateType)) {
                Collections.swap(colPredicates, 0, 1);
            }

            // calculate time points first
            ConstantOperator leftDayBegin = (ConstantOperator) leftDayBeginPredicate.getChild(1);
            ConstantOperator rightDayEnd = (ConstantOperator) rightDayEndPredicate.getChild(1);

            ConstantOperator leftMonthBegin = ScalarOperatorFunctions.monthsAdd(
                    ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar("month"), leftDayBegin),
                    ConstantOperator.createInt(1));
            ConstantOperator rightDayBegin =
                    ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar("month"), rightDayEnd);

            ConstantOperator yearBegin = ScalarOperatorFunctions.yearsAdd(
                    ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar("year"), leftDayBegin),
                    ConstantOperator.createInt(1));

            ConstantOperator rightMonthBegin =
                    ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar("year"), rightDayEnd);

            // 1.Selecting remaining days

            BinaryPredicateOperator leftDayEndPredicate = BinaryPredicateOperator.lt(curColumn, leftMonthBegin);
            BinaryPredicateOperator rightDayBeginPredicate = BinaryPredicateOperator.ge(curColumn, rightDayBegin);

            ScalarOperator leftDayPredicate = generateCompound(leftDayBeginPredicate, leftDayEndPredicate);
            ScalarOperator rightDayPredicate = generateCompound(rightDayBeginPredicate, rightDayEndPredicate);
            curResultPredicates.add(leftDayPredicate);
            curResultPredicates.add(rightDayPredicate);

            // 2.Selecting remaining months
            // wrap with data_trunct
            CallOperator monthOfDate =
                    buildDateTrunc(
                            Arrays.asList(ConstantOperator.createVarchar("month"), curColumn));
            BinaryPredicateOperator leftMonthBeginPredicate = BinaryPredicateOperator.ge(monthOfDate, leftMonthBegin);
            BinaryPredicateOperator leftMonthEndPredicate = BinaryPredicateOperator.lt(monthOfDate, yearBegin);

            BinaryPredicateOperator rightMonthBeginPredicate = BinaryPredicateOperator.ge(monthOfDate, rightMonthBegin);
            BinaryPredicateOperator rightMonthEndPredicate = BinaryPredicateOperator.lt(monthOfDate, rightDayBegin);

            ScalarOperator leftMonthPredicate = generateCompound(leftMonthBeginPredicate, leftMonthEndPredicate);
            ScalarOperator rightMonthPredicate = generateCompound(rightMonthBeginPredicate, rightMonthEndPredicate);
            curResultPredicates.add(leftMonthPredicate);
            curResultPredicates.add(rightMonthPredicate);

            // 3.Selecting remaining years
            CallOperator yearOfDate =
                    buildDateTrunc(Arrays.asList(ConstantOperator.createVarchar("year"), curColumn));

            BinaryPredicateOperator yearBeginPredicate = BinaryPredicateOperator.ge(yearOfDate, yearBegin);
            BinaryPredicateOperator yearEndPredicate = BinaryPredicateOperator.lt(yearOfDate, rightMonthBegin);
            ScalarOperator yearPredicate = generateCompound(yearBeginPredicate, yearEndPredicate);
            curResultPredicates.add(yearPredicate);

            ScalarOperator resultPredicate = Utils.compoundOr(curResultPredicates);
            resultPredicates.add(resultPredicate);
        }

        scan.setPredicate(Utils.compoundAnd(resultPredicates));
        return Lists.newArrayList(input);
    }

    private ScalarOperator generateCompound(BinaryPredicateOperator left, BinaryPredicateOperator right) {
        List<ScalarOperator> predicateChild = new ArrayList<>();
        predicateChild.add(left);
        predicateChild.add(right);
        return Utils.compoundAnd(predicateChild);
    }

}
