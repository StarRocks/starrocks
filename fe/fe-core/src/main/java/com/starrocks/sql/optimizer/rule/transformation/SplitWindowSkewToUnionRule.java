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
        // We do not duplicate the child since we can reuse it directly.
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
        // We need to duplicate the child to avoid conflicts in column references.
        BranchResult unskewedBranch = buildBranch(
                context,
                child,
                window,
                window.getPartitionExpressions(),
                unskewedPredicate,
                true
        );

        // 4. Build Union
        LogicalUnionOperator unionOp = buildUnionOperator(context, child, window, unskewedBranch);

        return Lists.newArrayList(OptExpression.create(unionOp, skewedBranch.root, unskewedBranch.root));
    }

    private LogicalUnionOperator buildUnionOperator(OptimizerContext context,
                                                    OptExpression child,
                                                    LogicalWindowOperator window,
                                                    BranchResult unskewedBranch) {
        List<ColumnRefOperator> outputColumns = Lists.newArrayList();
        List<ColumnRefOperator> skewedChildColumns = Lists.newArrayList();
        List<ColumnRefOperator> unskewedChildColumns = Lists.newArrayList();

        // Combine window output columns and child pass-through columns
        List<ColumnRefOperator> allColumns = Lists.newArrayList(window.getWindowCall().keySet());
        if (child.getOutputColumns() != null) {
            allColumns.addAll(child.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory()));
        }

        // Populate lists based on mappings
        for (ColumnRefOperator col : allColumns) {
            outputColumns.add(col);
            // For the skewed branch, we use the original columns.
            skewedChildColumns.add(col);
            unskewedChildColumns.add(unskewedBranch.columnMapping.get(col));
        }

        return new LogicalUnionOperator(outputColumns, List.of(skewedChildColumns, unskewedChildColumns), true);
    }

    private BranchResult buildBranch(OptimizerContext context,
                                     OptExpression child,
                                     LogicalWindowOperator originalWindow,
                                     List<ScalarOperator> partitionExprs,
                                     ScalarOperator predicate,
                                     boolean needsDuplication) {

        OptExpression filterExpr = OptExpression.create(new LogicalFilterOperator(predicate), child);
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();

        LogicalWindowOperator.Builder windowBuilder = new LogicalWindowOperator.Builder()
                .setAnalyticWindow(originalWindow.getAnalyticWindow())
                .setUseHashBasedPartition(partitionExprs.isEmpty() && originalWindow.isUseHashBasedPartition())
                .setIsSkewed(partitionExprs.isEmpty() && originalWindow.isSkewed())
                .setInputIsBinary(originalWindow.isInputIsBinary());

        if (needsDuplication) {
            OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
            filterExpr = duplicator.duplicate(filterExpr);

            windowBuilder.setPartitionExpressions(rewriteExpressions(partitionExprs, duplicator));
            windowBuilder.setOrderByElements(rewriteOrderings(originalWindow.getOrderByElements(), duplicator));
            windowBuilder.setEnforceSortColumns(rewriteOrderings(originalWindow.getEnforceSortColumns(), duplicator));

            Map<ColumnRefOperator, CallOperator> newWindowCalls = Maps.newHashMap();
            ColumnRefFactory factory = context.getColumnRefFactory();

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : originalWindow.getWindowCall().entrySet()) {
                ColumnRefOperator originalCol = entry.getKey();
                CallOperator originalCall = entry.getValue();

                CallOperator newCall = (CallOperator) duplicator.rewriteAfterDuplicate(originalCall);
                // Create a new output column for the duplicated window function
                ColumnRefOperator newCol = factory.create(newCall.toString(), originalCol.getType(), originalCol.isNullable());

                newWindowCalls.put(newCol, newCall);
                mapping.put(originalCol, newCol);
            }

            windowBuilder.setWindowCall(newWindowCalls);
            mapping.putAll(duplicator.getColumnMapping());
        } else {
            windowBuilder.setPartitionExpressions(partitionExprs);
            windowBuilder.setOrderByElements(originalWindow.getOrderByElements());
            windowBuilder.setEnforceSortColumns(originalWindow.getEnforceSortColumns());
            windowBuilder.setWindowCall(originalWindow.getWindowCall());
        }

        return new BranchResult(OptExpression.create(windowBuilder.build(), filterExpr), mapping);
    }

    private List<ScalarOperator> rewriteExpressions(List<ScalarOperator> exprs, OptExpressionDuplicator duplicator) {
        if (exprs == null) {
            return null;
        }
        return exprs.stream()
                .map(duplicator::rewriteAfterDuplicate)
                .toList();
    }

    private List<Ordering> rewriteOrderings(List<Ordering> orderings, OptExpressionDuplicator duplicator) {
        if (orderings == null) {
            return null;
        }
        return orderings.stream()
                .map(o -> new Ordering(
                        duplicator.getColumnMapping().get(o.getColumnRef()),
                        o.isAscending(),
                        o.isNullsFirst())
                )
                .toList();
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
            var skewInfo = DataSkew.getColumnSkewInfo(statistics, colStat, DataSkew.Thresholds.withMcvLimit(1));
            if (skewInfo.isSkewed()) {

                if (skewInfo.type() == DataSkew.SkewType.SKEWED_NULL) {
                    return List.of(new SkewedInfo(col, ConstantOperator.createNull(col.getType())));
                }
                if (skewInfo.maybeMcvs().isPresent()) {
                    return skewInfo.maybeMcvs().get().stream().map(p -> (new SkewedInfo(col,
                            ConstantOperator.createVarchar(p.first)))).toList();
                }
            }
        }
        return Collections.emptyList();
    }
}
