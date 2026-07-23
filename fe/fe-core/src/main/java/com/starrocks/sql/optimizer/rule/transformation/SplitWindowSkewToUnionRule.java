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
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.skew.DataSkew;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_SPLIT_WINDOW_SKEW;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.NOT;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.OR;

/*
 * Rule Objective:
 *
 * Optimize a Window operator with a skewed partition column by splitting it into a UNION of multiple branches.
 * Each skewed value will be handled by a branch (where partitioning can often be eliminated or optimized).
 * The rest of the values will be handled together by another branch.
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
 *                                      /   \  \ \__________________
 *                                     /     \  \____________       \
 *                         +----------+     +----------+     \       +---------+
 *                         | Window   |     | Window   |     ...     | Window  |
 *                         | (No Ptn) |     | (No Ptn) |             |(Org Ptn)|
 *                         +---+------+     +----+-----+             +---+-----+
 *                             |                 |                       |
 *                             |                 |                       |
 *                        +----+-----+      +----+-----+          +------+---------------------------------+
 *                        |  Filter  |      |  Filter  |          |         Filter                         |
 *                        |  (p=V1)  |      |  (p=V2)  |          | (p!=V1 AND p!=V2 AND ...) OR p IS NULL |
 *                        +----+-----+      +----+-----+          +----------------------+-----------------+
 *                             |                 |                                       |
 *                             v                 v                                       v
 *                         +-------+         +-------+                               +-------+
 *                         | Child |         | Child |                               | Child |
 *                         +-------+         +-------+                               +-------+
 * Where:
 *   - p: partition column
 *   - V: skewed value
 *   - V_i: i-th skewed value
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
            if (lwo.isOpRuleBitSet(OP_SPLIT_WINDOW_SKEW)) {
                return false;
            }

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

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SkewedInfo that = (SkewedInfo) o;
            return column.equivalent(that.column) && value.equivalent(that.value);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(column, value);
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalWindowOperator window = (LogicalWindowOperator) input.getOp();
        OptExpression child = input.inputAt(0);
        Statistics statistics = child.getStatistics();
        SessionVariable sessionVariable = context.getSessionVariable();
        final int maxBranchCount = sessionVariable.getSplitWindowSkewToUnionMaxSkewedBranchCount();

        // Step 1: Identify Skew
        // First check for explicit skew hint from user (takes precedence over statistics)
        List<SkewedInfo> skewedInfos = findSkewedPartitionFromHint(window)
                .stream().distinct().limit(maxBranchCount).toList();

        // If no hint is provided by the user, fall back to statistics-based detection
        if (skewedInfos.isEmpty()) {
            skewedInfos = findSkewedPartition(window.getPartitionExpressions(), statistics, sessionVariable)
                    .stream().distinct().limit(maxBranchCount).toList();
        }

        if (skewedInfos.isEmpty()) {
            return Collections.emptyList();
        }

        // Step 2: Build Predicates for Branches
        boolean hasNullSkew = false;
        List<ScalarOperator> skewedPredicates = new ArrayList<>();

        for (SkewedInfo skew : skewedInfos) {
            if (skew.value.isNull()) {
                hasNullSkew = true;
                skewedPredicates.add(new IsNullPredicateOperator(skew.column));
            } else {
                skewedPredicates.add(new BinaryPredicateOperator(BinaryType.EQ, skew.column, skew.value));
            }
        }

        ScalarOperator unskewedPredicate =
                buildUnskewedPredicate(skewedInfos.get(0).column, skewedPredicates, hasNullSkew);

        // Step 3: Build Unskewed Branch
        // A single branch will be built which excludes all the skewed values
        BranchResult unskewedBranch = buildUnskewedBranch(
                child,
                window,
                window.getPartitionExpressions(),
                unskewedPredicate
        );

        // Step 4: Build Skewed Branches
        // A new branch will be built for each skewed predicate
        List<BranchResult> skewedBranches = skewedPredicates.stream().map(
                predicate -> buildSkewedBranch(
                        context,
                        child,
                        window,
                        predicate)).toList();

        // Step 5: Build Union
        OptExpression unionExpr = buildUnionOperator(context, window, child, unskewedBranch, skewedBranches);
        // The split builds a fresh UNION subtree and the duplicated branch starts without logical properties or
        // statistics. Derive logical properties first so statistics estimators can inspect output columns, then
        // calculate fresh statistics for the duplicated branch.
        deriveLogicalProperty(unionExpr);
        Utils.calculateStatistics(unionExpr, context);

        return Lists.newArrayList(unionExpr);
    }

    /**
     * Performs a left-fold on a non-empty list of predicates and connects them with the given logical connective (AND/OR).
     * Examples:
     * [a, b, c, d] yields "((a OR b) OR c) OR d"
     * [a, b]       yields "a OR b"
     * [a]          yields "a"
     * []           throws
     */
    private ScalarOperator buildLogicalConnective(CompoundPredicateOperator.CompoundType connector,
                                                  List<ScalarOperator> predicates) {
        return predicates.stream().reduce((a, b) -> new CompoundPredicateOperator(connector, a, b)).orElseThrow();
    }


    private ScalarOperator buildUnskewedPredicate(ColumnRefOperator skewColumn,
                                                  List<ScalarOperator> skewedPredicates,
                                                  boolean hasNullSkew) {
        // We build a single predicate for the unskewed branch by negating the skewed predicates.
        // For example, if the skewed predicates are {"col = 1", "col = 2"}, then the unskewed predicate will be
        // "NOT (col = 1 OR col = 2)" which is equivalent to "col != 1 AND col != 2".
        // De Morgan law will be applied by the query optimizer in the ruleset PUSH_DOWN_PREDICATE_RULES after this rule.

        // The tricky part is handling nulls correctly. Standard SQL equality comparison filters out NULLs.
        // For example, neither the predicate "col = 1" nor "col != 1" matches the nulls values.
        // Instead, "col IS NULL" or "col IS NOT NULL" needs to be used to include/exclude nulls explicitly.

        var notAnySkewedPredicate = new CompoundPredicateOperator(NOT, buildLogicalConnective(OR, skewedPredicates));

        // If there is no null skew, null values need to be explicitly included in the unskewed branch.
        var skewColumnIsNull = new IsNullPredicateOperator(skewColumn);
        return hasNullSkew
                ? notAnySkewedPredicate
                : new CompoundPredicateOperator(OR, skewColumnIsNull, notAnySkewedPredicate);
    }

    private OptExpression buildUnionOperator(OptimizerContext context,
                                             LogicalWindowOperator originalWindow,
                                             OptExpression originalChild,
                                             BranchResult unskewedBranch,
                                             List<BranchResult> skewedBranches) {

        // Determine the output columns of the original window operator before rewrite
        List<ColumnRefOperator> allColumns = new ArrayList<>(originalWindow.getWindowCall().keySet());
        if (originalChild.getOutputColumns() != null) {
            allColumns.addAll(originalChild.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory()));
        }

        // The output columns of the UNION operator needs to be the same as the original window operator's output columns,
        // since the UNION becomes the topmost operator in the sub-plan after the rewrite.
        List<ColumnRefOperator> outputColumns = new ArrayList<>(allColumns);

        // The first input for the UNION operator is the root of the unskewed branch, which uses the original columns.
        List<OptExpression> inputRoots = Lists.newArrayList(unskewedBranch.root);
        List<List<ColumnRefOperator>> inputColumns = Lists.newArrayList(List.of(allColumns));

        // Rest of the inputs for the UNION operator are the roots of the skewed branches, which use the mapped columns.
        for (BranchResult branch : skewedBranches) {
            inputRoots.add(branch.root);
            inputColumns.add(allColumns.stream().map(
                    col -> branch.columnMapping.get(col)).toList());
        }

        return OptExpression.create(
                new LogicalUnionOperator(outputColumns, inputColumns, true), inputRoots);
    }

    private BranchResult buildUnskewedBranch(OptExpression child,
                                             LogicalWindowOperator originalWindow,
                                             List<ScalarOperator> partitionExpressions,
                                             ScalarOperator predicate) {
        // We reuse the original query plan for the unskewed branch, keep the original partitioning,
        // and add a filter to exclude the skewed values.

        LogicalWindowOperator.Builder windowBuilder = new LogicalWindowOperator.Builder()
                .withOperator(originalWindow)
                .setSkewColumn(null) // unset to prevent the rule from being reapplied infinitely
                .setSkewValues(List.of())
                .setUseHashBasedPartition(originalWindow.isUseHashBasedPartition())
                .setPartitionExpressions(partitionExpressions)
                .setIsSkewed(false);

        OptExpression filterExpr = OptExpression.create(new LogicalFilterOperator(predicate), child);
        LogicalWindowOperator branchWindow = windowBuilder.build();
        branchWindow.setOpRuleBit(OP_SPLIT_WINDOW_SKEW);
        return new BranchResult(OptExpression.create(branchWindow, filterExpr), Maps.newHashMap());
    }

    private BranchResult buildSkewedBranch(OptimizerContext context,
                                           OptExpression child,
                                           LogicalWindowOperator originalWindow,
                                           ScalarOperator predicate) {

        LogicalWindowOperator.Builder windowBuilder = new LogicalWindowOperator.Builder()
                .withOperator(originalWindow)
                .setSkewColumn(null) // unset to prevent the rule from being reapplied infinitely
                .setSkewValues(List.of())
                .setPartitionExpressions(List.of())
                .setUseHashBasedPartition(originalWindow.isUseHashBasedPartition())
                .setIsSkewed(originalWindow.isSkewed());

        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(context.getColumnRefFactory(), context);
        ColumnRefFactory columnFactory = context.getColumnRefFactory();

        OptExpression filterExpr = duplicator.duplicate(OptExpression.create(new LogicalFilterOperator(predicate), child));

        windowBuilder.setOrderByElements(rewriteOrderings(originalWindow.getOrderByElements(), duplicator));
        windowBuilder.setEnforceSortColumns(rewriteOrderings(originalWindow.getEnforceSortColumns(), duplicator));

        Map<ColumnRefOperator, ColumnRefOperator> columnMapping = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> newWindowCalls = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : originalWindow.getWindowCall().entrySet()) {
            ColumnRefOperator originalCol = entry.getKey();
            CallOperator originalCall = entry.getValue();

            CallOperator newCall = (CallOperator) duplicator.rewriteAfterDuplicate(originalCall);
            // Create a new output column for the duplicated window function
            ColumnRefOperator newCol = columnFactory.create(newCall.toString(), originalCol.getType(), originalCol.isNullable());
            newWindowCalls.put(newCol, newCall);
            columnMapping.put(originalCol, newCol);
        }

        windowBuilder.setWindowCall(newWindowCalls);
        columnMapping.putAll(duplicator.getColumnMapping());

        LogicalWindowOperator branchWindow = windowBuilder.build();
        branchWindow.setOpRuleBit(OP_SPLIT_WINDOW_SKEW);
        return new BranchResult(OptExpression.create(branchWindow, filterExpr), columnMapping);
    }

    private void deriveLogicalProperty(OptExpression root) {
        if (root.getLogicalProperty() != null) {
            return;
        }

        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }
        root.deriveLogicalPropertyItself();
    }

    private List<Ordering> rewriteOrderings(List<Ordering> orderings, OptExpressionDuplicator duplicator) {
        if (orderings == null) {
            return Collections.emptyList();
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

    /**
     * Check for explicit skew hint from user: [skew|t.column(value)]
     * This takes precedence over statistics-based detection.
     */
    private List<SkewedInfo> findSkewedPartitionFromHint(LogicalWindowOperator window) {
        ScalarOperator skewColumn = window.getSkewColumn();
        List<ScalarOperator> skewValues = window.getSkewValues();

        if (skewColumn == null || skewValues == null || skewValues.isEmpty()) {
            return Collections.emptyList();
        }

        if (!(skewColumn instanceof ColumnRefOperator col)) {
            return Collections.emptyList();
        }

        // Verify that the skewed column is part of the partition expressions
        if (!window.getPartitionExpressions().contains(col)) {
            throw new SemanticException("Can't find skew column");
        }

        // Validate that each skew value is type-compatible with the column and cast to the column's type
        return skewValues.stream()
                .map(v -> {
                    if (!(v instanceof ConstantOperator c)) {
                        throw new SemanticException("Window skew hint values must be constant");
                    }
                    if (c.isNull()) {
                        return new SkewedInfo(col, ConstantOperator.createNull(col.getType()));
                    }
                    Optional<ConstantOperator> cast = c.castTo(col.getType());
                    if (cast.isEmpty()) {
                        throw new SemanticException("Window skew hint value type mismatch: " +
                                c.getType() + " vs " + col.getType());
                    }
                    return new SkewedInfo(col, cast.get());
                })
                .toList();
    }

    /**
     * Retrieve a list of skewed values for the partition column based on statistics.
     * The values are sorted by their individual skew factor (most skewed values first).
     * Null skew and MCV skew are both included in the result.
     */
    private List<SkewedInfo> findSkewedPartition(List<ScalarOperator> partitionExprs, Statistics statistics,
                                                 SessionVariable sessionVariable) {
        if (statistics == null) {
            return Collections.emptyList();
        }
        var op = partitionExprs.get(0);
        if (op instanceof ColumnRefOperator col) {
            if (!statistics.getColumnStatistics().containsKey(col)) {
                return Collections.emptyList();
            }

            ColumnStatistic colStat = statistics.getColumnStatistic(col);

            DataSkew.Thresholds thresholds = new DataSkew.Thresholds(
                    sessionVariable.getSkewJoinOptimizeUseMCVCount(),
                    sessionVariable.getSkewJoinDataSkewThreshold());
            double singleValueThreshold = sessionVariable.getSkewJoinMcvSingleThreshold();

            DataSkew.SkewCandidates candidates =
                    DataSkew.getSkewCandidates(statistics, colStat, thresholds, singleValueThreshold);

            record SkewedInfoWithFactor(SkewedInfo skewInfo, double skewFactor) {
            }
            var skewedInfos = new ArrayList<SkewedInfoWithFactor>();

            if (candidates.nullSkewFactor().isPresent()) {
                var factor = candidates.nullSkewFactor().get();
                var nullSkewInfo = new SkewedInfo(col, ConstantOperator.createNull(col.getType()));
                skewedInfos.add(new SkewedInfoWithFactor(nullSkewInfo, factor));
            }

            for (var mcv : candidates.mcvs()) {
                var value = ConstantOperator.createVarchar(mcv.first).castTo(col.getType());
                if (value.isPresent()) {
                    var factor = (double) mcv.second / Math.max(1.0, statistics.getOutputRowCount());
                    var mcvSkewInfo = new SkewedInfo(col, value.get());
                    skewedInfos.add(new SkewedInfoWithFactor(mcvSkewInfo, factor));
                }
            }

            var descending = Comparator.comparing(SkewedInfoWithFactor::skewFactor).reversed();
            return skewedInfos.stream().sorted(descending).map(e -> e.skewInfo).toList();
        }
        return Collections.emptyList();
    }
}
