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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.skew.DataSkew;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SplitWindowSkewToUnionRule extends TransformationRule {
    private static final SplitWindowSkewToUnionRule INSTANCE = new SplitWindowSkewToUnionRule();

    private SplitWindowSkewToUnionRule() {
        super(RuleType.TF_SPLIT_WINDOW_SKEW, Pattern.create(OperatorType.LOGICAL_WINDOW));
    }

    public static SplitWindowSkewToUnionRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (input.getOp() instanceof LogicalWindowOperator lwo) {
            List<ScalarOperator> partitionExprs = lwo.getPartitionExpressions();

            // Rule only applies if there is exactly one partition expression,
            // and that expression is a direct ColumnReference (not a function or expression).
            return partitionExprs != null
                    && partitionExprs.size() == 1
                    && lwo.getOrderByElements() != null
                    && !lwo.getOrderByElements().isEmpty();
        }
        return false;
    }

    public static class SkewedInfo {
        ColumnRefOperator column;
        ConstantOperator value;

        public SkewedInfo(ColumnRefOperator column, ConstantOperator value) {
            this.column = column;
            this.value = value;
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalWindowOperator window = (LogicalWindowOperator) input.getOp();
        OptExpression child = input.inputAt(0);
        Statistics statistics = child.getStatistics();

        // 1. Identify Skew
        // We look for a partition column that has a specific value causing data skew.
        var skewedInfo = findSkewedPartition(window.getPartitionExpressions(), statistics);

        //todo (m.bogusz) in theory we could have multiple skewed values, but for now we only handle one
        if (skewedInfo.size() != 1) {
            return Collections.emptyList();
        }

        var skewedColumn = skewedInfo.get(0).column;
        PredicateOperator skewedPredicate;
        PredicateOperator unskewedPredicate;

        if (skewedInfo.get(0).value.isNull()) {
            skewedPredicate = new IsNullPredicateOperator(skewedColumn);
            unskewedPredicate = new IsNullPredicateOperator(true, skewedColumn);
        } else {
            skewedPredicate = new BinaryPredicateOperator(BinaryType.EQ, skewedColumn, skewedInfo.get(0).value);
            // In the unskewed branch, we need to include NULL values if the skewed value is NOT NULL.
            // Since standard SQL inequality (col != value) filters out NULLs, we must explicitly add 'OR col IS NULL'.
            unskewedPredicate = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    List.of(new BinaryPredicateOperator(BinaryType.NE, skewedColumn, skewedInfo.get(0).value),
                            new IsNullPredicateOperator(false, skewedColumn)));
        }
        // 2. Build Skewed Branch (where p == skewed_value)
        // In this branch, we remove the partitioning to handle the skewed data separately.
        BranchResult skewedBranch = buildBranch(
                context,
                child,
                window,
                List.of(),
                skewedPredicate
        );

        // 3. Build Unskewed Branch (where p != skewed_value)
        // We keep the original partition expressions for the rest of the data.
        BranchResult unskewedBranch = buildBranch(
                context,
                child,
                window,
                window.getPartitionExpressions(),
                unskewedPredicate
        );

        // 4. Build Union
        // We need to align the output columns of the Union with the original Window operator.
        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        List<ColumnRefOperator> skewedChildColumns = Lists.newArrayList();
        List<ColumnRefOperator> unskewedChildColumns = Lists.newArrayList();

        // 4a. Map Window Function Results
        // The original window operator produced specific ColumnRefs for function calls.
        // We map these original refs to the new refs generated in the split branches.
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : window.getWindowCall().entrySet()) {
            ColumnRefOperator originalCol = entry.getKey();
            outputColumns.add(originalCol);
            skewedChildColumns.add(skewedBranch.newWindowColMap.get(originalCol));
            unskewedChildColumns.add(unskewedBranch.newWindowColMap.get(originalCol));
        }

        // 4b. Map Pass-through Columns
        // Columns from the child that are passed through the window operator.
        // Since we didn't clone the child expression, the ColumnRefs are identical.
        if (child.getOutputColumns() != null) {
            for (ColumnRefOperator col : child.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory())) {
                // Avoid duplicates if child output overlaps with window calls (unlikely but safe)
                if (!outputColumns.contains(col)) {
                    outputColumns.add(col);
                    skewedChildColumns.add(col);
                    unskewedChildColumns.add(col);
                }
            }
        }

        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        childOutputColumns.add(skewedChildColumns);
        childOutputColumns.add(unskewedChildColumns);

        LogicalUnionOperator unionOp = new LogicalUnionOperator(outputColumns, childOutputColumns, true);
        return Lists.newArrayList(OptExpression.create(unionOp, skewedBranch.root, unskewedBranch.root));
    }

    private BranchResult buildBranch(OptimizerContext context,
                                     OptExpression child,
                                     LogicalWindowOperator originalWindow,
                                     List<ScalarOperator> partitionExprs,
                                     ScalarOperator predicate) {

        // Create Filter
        LogicalFilterOperator filterOp = new LogicalFilterOperator(predicate);
        OptExpression filterExpr = OptExpression.create(filterOp, child);

        // Create Window
        Map<ColumnRefOperator, CallOperator> newWindowCalls = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();
        ColumnRefFactory factory = context.getColumnRefFactory();

        // We must generate new ColumnRefs for the window results in this branch
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : originalWindow.getWindowCall().entrySet()) {
            ColumnRefOperator originalCol = entry.getKey();
            CallOperator originalCall = entry.getValue();

            ColumnRefOperator newCol = factory.create(originalCol.getName(), originalCol.getType(), originalCol.isNullable());
            newWindowCalls.put(newCol, originalCall);
            mapping.put(originalCol, newCol);
        }

        LogicalWindowOperator newWindow = new LogicalWindowOperator.Builder()
                .setWindowCall(newWindowCalls)
                .setPartitionExpressions(partitionExprs)
                .setOrderByElements(originalWindow.getOrderByElements())
                .setEnforceSortColumns(originalWindow.getEnforceSortColumns())
                .setAnalyticWindow(originalWindow.getAnalyticWindow())
                .build();

        OptExpression windowExpr = OptExpression.create(newWindow, filterExpr);

        return new BranchResult(windowExpr, mapping);
    }

    private static class BranchResult {
        OptExpression root;
        Map<ColumnRefOperator, ColumnRefOperator> newWindowColMap;

        public BranchResult(OptExpression root, Map<ColumnRefOperator, ColumnRefOperator> newWindowColMap) {
            this.root = root;
            this.newWindowColMap = newWindowColMap;
        }
    }

    private List<SkewedInfo> findSkewedPartition(List<ScalarOperator> partitionExprs, Statistics statistics) {
        if (statistics == null) {
            return Collections.emptyList();
        }
        var op = partitionExprs.get(0);
        if (op instanceof ColumnRefOperator col) {
            if (!statistics.getColumnStatistics().containsKey(col)) {
                return Collections.emptyList();
            }

            ColumnStatistic colStat = statistics.getColumnStatistic(col);
            var skewInfo = DataSkew.getColumnSkewInfo(statistics, colStat);
            if (skewInfo.isSkewed()) {

                if (skewInfo.type() == DataSkew.SkewType.SKEWED_NULL) {
                    return List.of(new SkewedInfo(col, ConstantOperator.createNull(col.getType())));
                }
                if (skewInfo.maybeMcvs().isPresent()) {
                    return skewInfo.maybeMcvs().get().stream().map(p -> (new SkewedInfo(col,
                            ConstantOperator.createVarchar(p.first)))).collect(Collectors.toList());
                }
            }
        }
        return Collections.emptyList();
    }
}
