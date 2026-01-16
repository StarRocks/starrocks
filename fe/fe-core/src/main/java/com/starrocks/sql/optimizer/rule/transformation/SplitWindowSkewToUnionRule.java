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
import com.starrocks.sql.optimizer.base.Ordering;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.skew.DataSkew;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/*
 * Rule Objective:
 *
 * Optimize a Window operator with a skewed partition column by splitting it into a UNION of two branches.
 * One branch handles the skewed value (where partitioning can often be eliminated or optimized),
 * and the other handles the remaining data.
 *
 * Transform the following SQL:
 *   SELECT row_number() OVER (PARTITION BY k1 ORDER BY v1)
 *   FROM t
 *
 * Into (assuming 'k1' is skewed with value 'A'):
 *   SELECT row_number() OVER (ORDER BY v1)
 *   FROM t
 *   WHERE k1 = 'A'
 *   UNION ALL
 *   SELECT row_number() OVER (PARTITION BY k1 ORDER BY v1)
 *   FROM t
 *   WHERE k1 != 'A' OR k1 IS NULL
 *
 *
 *                                     +--------+
 *                                     | Window |
 *                                     +--------+
 *                                         |
 *                                         v
 *                                     +-------+
 *                                     | Child |
 *                                     +-------+
 *
 *                                         |
 *                                         v
 *
 *                                     +-------+
 *                                     | UNION |
 *                                     +-------+
 *                                      /     \
 *                                     /       \
 *                           +--------+         +--------+
 *                           | Window |         | Window |
 *                           | (No Ptn)|        |(Org Ptn)|
 *                           +--------+         +--------+
 *                               |                  |
 *                               |                  |
 *                          +----+-----+       +----+-------------------+
 *                          |  Filter  |       |         Filter         |
 *                          |   (p=V)  |       | (p!=V OR p IS NULL)    |
 *                          +----+-----+       +----+-------------------+
 *                               |                  |
 *                               v                  v
 *                           +-------+          +-------+
 *                           | Child |          | Child |
 *                           +-------+          +-------+
 *
 * Where:
 *   - p: partition column
 *   - V: skewed value
 */

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
        List<SkewedInfo> skewedInfos = findSkewedPartition(window.getPartitionExpressions(), statistics);

        //todo (m.bogusz) in theory we could have multiple skewed values, but for now we only handle one
        if (skewedInfos.size() != 1) {
            return Collections.emptyList();
        }

        SkewedInfo skewInfo = skewedInfos.get(0);
        ColumnRefOperator skewedColumn = skewInfo.column;
        ConstantOperator skewedValue = skewInfo.value;

        PredicateOperator skewedPredicate;
        PredicateOperator unskewedPredicate;

        if (skewedValue.isNull()) {
            skewedPredicate = new IsNullPredicateOperator(skewedColumn);
            unskewedPredicate = new IsNullPredicateOperator(true, skewedColumn);
        } else {
            skewedPredicate = new BinaryPredicateOperator(BinaryType.EQ, skewedColumn, skewedValue);
            // In the unskewed branch, we need to include NULL values if the skewed value is NOT NULL.
            // Since standard SQL inequality (col != value) filters out NULLs, we must explicitly add 'OR col IS NULL'.
            unskewedPredicate = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    List.of(new BinaryPredicateOperator(BinaryType.NE, skewedColumn, skewedValue),
                            new IsNullPredicateOperator(false, skewedColumn)));
        }

        // 2. Build Skewed Branch (where p == skewed_value)
        // In this branch, we remove the partitioning to handle the skewed data separately.
        BranchResult skewedBranch = buildBranch(
                context,
                child,
                window,
                List.of(),
                skewedPredicate,
                false
        );

        // 3. Build Unskewed Branch (where p != skewed_value)
        // We keep the original partition expressions for the rest of the data.
        BranchResult unskewedBranch = buildBranch(
                context,
                child,
                window,
                window.getPartitionExpressions(),
                unskewedPredicate,
                true
        );

        // 4. Build Union
        LogicalUnionOperator unionOp = buildUnionOperator(context, child, window, skewedBranch, unskewedBranch);

        return Lists.newArrayList(OptExpression.create(unionOp, skewedBranch.root, unskewedBranch.root));
    }

    private LogicalUnionOperator buildUnionOperator(OptimizerContext context,
                                                    OptExpression child,
                                                    LogicalWindowOperator window,
                                                    BranchResult skewedBranch,
                                                    BranchResult unskewedBranch) {
        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        List<ColumnRefOperator> skewedChildColumns = Lists.newArrayList();
        List<ColumnRefOperator> unskewedChildColumns = Lists.newArrayList();

        // 1. Map Window Function Results
        for (ColumnRefOperator originalCol : window.getWindowCall().keySet()) {
            outputColumns.add(originalCol);
            skewedChildColumns.add(skewedBranch.columnMapping.get(originalCol));
            unskewedChildColumns.add(unskewedBranch.columnMapping.get(originalCol));
        }

        // 2. Map Pass-through Columns
        if (child.getOutputColumns() != null) {
            for (ColumnRefOperator col : child.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory())) {
                outputColumns.add(col);
                skewedChildColumns.add(skewedBranch.columnMapping.getOrDefault(col, col));
                unskewedChildColumns.add(unskewedBranch.columnMapping.getOrDefault(col, col));
            }
        }

        return new LogicalUnionOperator(outputColumns, List.of(skewedChildColumns, unskewedChildColumns), true);
    }

    private BranchResult buildBranch(OptimizerContext context,
                                     OptExpression child,
                                     LogicalWindowOperator originalWindow,
                                     List<ScalarOperator> partitionExprs,
                                     ScalarOperator predicate,
                                     boolean needsDuplication) {

        // Create Filter
        LogicalFilterOperator filterOp = new LogicalFilterOperator(predicate);
        OptExpression filterExpr = OptExpression.create(filterOp, child);
        OptExpressionDuplicator duplicator = null;

        List<ScalarOperator> currentPartitionExprs = partitionExprs;
        List<Ordering> currentOrderByElements = originalWindow.getOrderByElements();
        List<Ordering> currentEnforceSortColumns = originalWindow.getEnforceSortColumns();

        if (needsDuplication) {
            duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
            filterExpr = duplicator.duplicate(filterExpr);

            final OptExpressionDuplicator finalDuplicator = duplicator;

            // Rewrite partition expressions
            currentPartitionExprs = partitionExprs.stream()
                    .map(duplicator::rewriteAfterDuplicate)
                    .collect(Collectors.toList());

            // Rewrite order by elements
            if (originalWindow.getOrderByElements() != null) {
                currentOrderByElements = originalWindow.getOrderByElements().stream()
                        .map(o -> new Ordering(
                                finalDuplicator.getColumnMapping().get(o.getColumnRef()),
                                o.isAscending(),
                                o.isNullsFirst())
                        )
                        .collect(Collectors.toList());
            }

            // Rewrite enforce sort columns
            if (originalWindow.getEnforceSortColumns() != null) {
                currentEnforceSortColumns = originalWindow.getEnforceSortColumns().stream()
                        .map(o -> new Ordering(
                                finalDuplicator.getColumnMapping().get(o.getColumnRef()),
                                o.isAscending(),
                                o.isNullsFirst())
                        )
                        .collect(Collectors.toList());
            }
        }

        // Create Window
        Map<ColumnRefOperator, CallOperator> newWindowCalls = Maps.newHashMap();
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();
        ColumnRefFactory factory = context.getColumnRefFactory();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : originalWindow.getWindowCall().entrySet()) {
            ColumnRefOperator originalCol = entry.getKey();
            CallOperator originalCall = entry.getValue();

            ColumnRefOperator newCol = factory.create(originalCol.getName(), originalCol.getType(), originalCol.isNullable());
            // Rewrite CallOperator arguments for duplicated branch
            CallOperator newCall = originalCall;
            if (duplicator != null) {
                newCall = (CallOperator) duplicator.rewriteAfterDuplicate(originalCall);
            }

            newWindowCalls.put(newCol, newCall);
            mapping.put(originalCol, newCol);
        }

        if (duplicator != null) {
            mapping.putAll(duplicator.getColumnMapping());
        }

        LogicalWindowOperator newWindow = new LogicalWindowOperator.Builder()
                .setWindowCall(newWindowCalls)
                .setPartitionExpressions(currentPartitionExprs)
                .setOrderByElements(currentOrderByElements)
                .setAnalyticWindow(originalWindow.getAnalyticWindow())
                .setEnforceSortColumns(currentEnforceSortColumns)
                .setUseHashBasedPartition(partitionExprs.isEmpty() && originalWindow.isUseHashBasedPartition())
                .setIsSkewed(partitionExprs.isEmpty() && originalWindow.isSkewed())
                .setInputIsBinary(originalWindow.isInputIsBinary())
                .build();

        OptExpression windowExpr = OptExpression.create(newWindow, filterExpr);

        return new BranchResult(windowExpr, mapping);
    }

    private static class BranchResult {
        OptExpression root;
        Map<ColumnRefOperator, ColumnRefOperator> columnMapping;

        public BranchResult(OptExpression root, Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
            this.root = root;
            this.columnMapping = columnMapping;
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
