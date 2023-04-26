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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Eliminate apply by window function according to 「WinMagic : Subquery Elimination Using Window Aggregation」
 * Before:
 *                Filter1
 *                  |
 *                  |
 *                Project
 *                  |
 *                  |
 *                 Apply
 *                 / \
 *               /    \
 *             Leaf   Agg
 *
 * After:
 *               Project (some new mappings added into it)
 *                  |
 *                  |
 *                Filter1_1 (only subquery predicate from Filter1)
 *                  |
 *                  |
 *                Window
 *                  |
 *                  |
 *                Filter1_2 (other predicates from Filter1)
 *                  |
 *                  |
 *                 Leaf
 *
 * For example, Q1 can be rewritten to Q2
 * -- Q1
 * SELECT T1.X
 * FROM T1,T2
 * WHERE T1.y = T2.y AND
 * T2.name='ming' AND
 * T2.z relop (
 *     SELECT AGG(T2.w)
 *     FROM  T2
 *     WHERE T2.y = T1.y and T2.name='ming');
 *
 * -- Q2
 * SELECT V.x
 * FROM (
 *     SELECT T1.x, T2.z,
 *         AGG (T2.w) OVER (PARTITION BY T2.y) AS win_agg
 *     FROM T1, T2
 *     WHERE T1.y = T2.y
 *     AND T2.name='ming'
 * ) V
 * WHERE V.z relop win_agg
 *
 * In short, only when the predicate of the subquery is identical to the outer block, then the above transformation
 * can be performed.
 *
 * Given that it is difficult to find out the ideal transformation condition for sake of complexity, so I'll only
 * give a sequence of sufficient but not necessary conditions with the key observation that the row set of subquery
 * must be identical to the out block's, and any factors that contributing to the change should be excluded.
 *
 *  1. The aggregation function must have a window version, like sum/min/max/avg/count and any_value is a count example.
 *  2. The aggregation expr must NOT contain key word distinct, which will reduces the row set of the subquery.
 *  3. The subquery can NOT contain limit clause, which will reduces the row set of the subquery.
 *  4. Outer query must contain all the correlated conjuncts of subquery.
 *  5. The relations accessed by the subquery and outer block must be exactly the same.
 *      * 5.1 Table alias is NOT allowed, for sake of implementation simplicity.
 *  6. The predicate of subquery and outer block(except the conjuncts which have nothing to do with subquery) must
 *     be exactly the same.
 *  7. ONLY cross join is allowed in outer block and subquery for sake of implementation complexity, though all types
 *     of join can be supported technically.
 */
public class ScalarApply2AnalyticRule extends TransformationRule {

    private static final Set<String> WHITE_LIST =
            ImmutableSet.of(FunctionSet.COUNT, FunctionSet.SUM, FunctionSet.AVG, FunctionSet.MIN, FunctionSet.MAX);

    public ScalarApply2AnalyticRule() {
        super(RuleType.TF_SCALAR_APPLY_TO_ANALYTIC,
                Pattern.create(OperatorType.LOGICAL_FILTER)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)
                                .addChildren(Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF,
                                        OperatorType.LOGICAL_AGGR))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression apply = input.inputAt(0).inputAt(0);
        LogicalApplyOperator applyOp = (LogicalApplyOperator) apply.getOp();
        // Or-Scope is same with And-Scope
        return applyOp.isScalar() && !SubqueryUtils.containsCorrelationSubquery(apply);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Transformer transformer = new Transformer(context, input);
        return transformer.transform();
    }

    private static final class Transformer {
        private final OptimizerContext context;
        private final OptExpression filter;
        private final OptExpression project;
        private final OptExpression apply;
        private final OptExpression subAgg;
        private final LogicalFilterOperator filterOp;
        private final LogicalProjectOperator projectOp;
        private final LogicalApplyOperator applyOp;
        private final LogicalAggregationOperator subAggOp;

        // All logical operators of the applyOp's left/right subtree
        private final List<LogicalOperator> leftOps = Lists.newArrayList();
        private final List<LogicalOperator> rightOps = Lists.newArrayList();

        // ColumnRefMap which contains all the project's columnRefMap of the right subtree, and the mapping of the
        // higher node will be replaced by the lower node
        private final Map<ColumnRefOperator, ScalarOperator> rightColumnRefMap = Maps.newHashMap();

        // Equivalent ColumnRefOperator peer, key comes from subquery's relation while value comes from outer block's
        private final Map<ColumnRefOperator, ColumnRefOperator> peerColumnRefMap = Maps.newHashMap();

        // Outer table of the correlated subquery
        private Table correlatedOuterTable;

        // All the columnRefOperators of the outer block, i.e. left subtree of apply
        private Set<ColumnRefOperator> outerColumnRefOperators;

        // The conjunct of outer block that is identical with the subquery's correlated conjunct
        private final List<ScalarOperator> outerCorrelatedConjuncts = Lists.newArrayList();

        // The conjunct that contains the subquery
        private ScalarOperator outerSubqueryConjunct;

        private ScalarOperator predicateBeforeWindow;
        private ScalarOperator predicateAfterWindow;

        private LogicalWindowOperator windowOp;

        public Transformer(OptimizerContext context, OptExpression input) {
            this.context = context;
            this.filter = input;
            this.project = this.filter.inputAt(0);
            this.apply = this.project.inputAt(0);
            this.subAgg = this.apply.inputAt(1);
            this.filterOp = this.filter.getOp().cast();
            this.projectOp = this.project.getOp().cast();
            this.applyOp = this.apply.getOp().cast();
            this.subAggOp = this.subAgg.getOp().cast();
        }

        public List<OptExpression> transform() {
            try {
                initOps();
                if (!checkOperatorType()
                        || !checkJoinType()
                        || !checkAggregate()
                        || !checkReferencedTables()
                        || !checkPredicate()) {
                    return Collections.emptyList();
                }

                createWindowOp();

                rewriteOuterPredicate();

                return getNewExpressionRoot();
            } catch (UnsupportedOperationException e) {
                return Collections.emptyList();
            }
        }

        private void initOps() {
            collectInLevelOrder(apply.inputAt(0), leftOps);
            collectInLevelOrder(apply.inputAt(1), rightOps);

            // All the operators are collected in level order, so the mapping from upper project node will be
            // replaced with the lower project node
            rightOps.stream()
                    .filter(LogicalProjectOperator.class::isInstance)
                    .map(Operator::<LogicalProjectOperator>cast)
                    .map(LogicalProjectOperator::getColumnRefMap)
                    .forEach(rightColumnRefMap::putAll);
        }

        private boolean checkOperatorType() {
            return leftOps.stream()
                    .allMatch(op -> op instanceof LogicalScanOperator
                            || op instanceof LogicalLimitOperator
                            || op instanceof LogicalJoinOperator
                            || op instanceof LogicalProjectOperator)
                    && rightOps.stream()
                    .allMatch(op -> op instanceof LogicalScanOperator
                            || op instanceof LogicalJoinOperator
                            || op instanceof LogicalAggregationOperator
                            || op instanceof LogicalFilterOperator
                            || op instanceof LogicalProjectOperator)
                    && rightOps.stream().noneMatch(op -> op.getLimit() >= 0);
        }

        /**
         * Currently, only cross join is allowed, we can loosen this condition later to support other types of join
         */
        private boolean checkJoinType() {
            boolean outerAllCrossJoin = leftOps.stream()
                    .filter(LogicalJoinOperator.class::isInstance)
                    .map(Operator::<LogicalJoinOperator>cast)
                    .allMatch(joinOp -> joinOp.getJoinType().isCrossJoin());
            boolean subqueryAllCrossJoin = rightOps.stream()
                    .filter(LogicalJoinOperator.class::isInstance)
                    .map(Operator::<LogicalJoinOperator>cast)
                    .allMatch(joinOp -> joinOp.getJoinType().isCrossJoin());

            return outerAllCrossJoin && subqueryAllCrossJoin;
        }

        /**
         * The following requirements should have been met
         * 1. No other aggregate node allowed in the subquery
         * 2. All the aggregate functions must have corresponding analytic version,
         * which is defined in TRANSFORMABLE_AGGREGATE_FUNCTIONS
         * 3. All the aggregate shouldn't be distinct
         */
        private boolean checkAggregate() {
            if (rightOps.stream()
                    .filter(LogicalAggregationOperator.class::isInstance)
                    .count() > 1) {
                return false;
            }

            Collection<CallOperator> aggregations = subAggOp.getAggregations().values();
            if (aggregations.stream()
                    .anyMatch(callOp -> !WHITE_LIST.contains(callOp.getFnName()))) {
                return false;
            }

            return aggregations.stream().noneMatch(CallOperator::isDistinct);
        }

        /*
         * Check whether `outerTables = subqueryTables + correlatedOuterTable` satisfied
         * The above requirement must be required, otherwise subquery cannot be eliminated through window function
         * <p>
         * Multi-relations pointed to the same table are out of consideration, example as follows
         *
         * select * from t0, t0 as t1
         * where t0.v1 = t1.v1
         * and t0.v2 < 5 and t1.v2 > 10
         * and t0.v3 < (
         *     select max(t1.v3) from t0 as t1
         *     where t0.v1 = t1.v1 and t1.v2 > 10
         * )
         */
        private boolean checkReferencedTables() {
            List<Table> outerTables = getAllTables(leftOps);
            List<Table> subqueryTables = getAllTables(rightOps);

            Set<Long> outerTableIds = outerTables.stream()
                    .map(Table::getId)
                    .collect(Collectors.toSet());
            Set<Long> subqueryTableIds = subqueryTables.stream()
                    .map(Table::getId)
                    .collect(Collectors.toSet());
            if (outerTables.size() != outerTableIds.size()
                    || subqueryTables.size() != subqueryTableIds.size()) {
                return false;
            }

            if (outerTables.size() != subqueryTables.size() + 1) {
                return false;
            }

            List<Column> correlatedOuterColumns = applyOp.getCorrelationColumnRefs().stream()
                    .map(context.getColumnRefFactory()::getColumn)
                    .collect(Collectors.toList());
            outerTables.removeAll(subqueryTables);
            if (outerTables.size() != 1) {
                return false;
            }

            correlatedOuterTable = outerTables.get(0);

            for (Column correlatedOuterColumn : correlatedOuterColumns) {
                if (!Objects.equals(correlatedOuterColumn,
                        correlatedOuterTable.getColumn(correlatedOuterColumn.getName()))) {
                    return false;
                }
            }
            return true;
        }

        /**
         * E.g.
         * select * from t0, t1
         * where t0.v1 = t1.v4
         * and t0.v2 < 5
         * and t1.v5 > 10
         * and t0.v3 < (select max(v6) from t1 where t0.v1 = t1.v4 and t1.v5 >10 )
         * <p>
         * Technically, one of the sufficient condition in predicate is that we must guarantee that the subquery and
         * the outer block yields the same row set, so all the predicates in the outer block should exist in subquery,
         * except those not related to the subquery's table, so here comes the detailed requirements that should be met:
         * 1. correlated conjunct must exist in the outer block, e.g. t0.v1 = t1.v4
         * 2. conjuncts of outer block except those not related to the subquery must exist in the subquery, for example,
         * t0.v2 < 5 is not related to the subquery's table, t1.v5 > 10 is related to the subquery's table
         */
        private boolean checkPredicate() {
            List<ScalarOperator> outerConjuncts = Utils.extractConjuncts(filterOp.getPredicate());

            // First, check correlated predicate exists in outerConjuncts, and remove it if found
            // E.g. t0.v1 = t1.v4 in the above case
            List<ScalarOperator> subqueryCorrelationConjuncts =
                    Utils.extractConjuncts(applyOp.getCorrelationConjuncts());
            {
                Iterator<ScalarOperator> outerIt = outerConjuncts.iterator();
                Iterator<ScalarOperator> subIt = subqueryCorrelationConjuncts.iterator();
                while (outerIt.hasNext()) {
                    ScalarOperator outerConjunct = outerIt.next();

                    while (subIt.hasNext()) {
                        ScalarOperator correlationConjunct = subIt.next();
                        if (PredicateComparator.isIdentical(correlationConjunct, outerConjunct,
                                context.getColumnRefFactory())) {
                            outerCorrelatedConjuncts.add(outerConjunct);
                            outerIt.remove();
                            subIt.remove();
                            break;
                        }
                    }
                }
            }
            if (outerCorrelatedConjuncts.isEmpty() || !subqueryCorrelationConjuncts.isEmpty()) {
                return false;
            }

            // Second, remove the conjunct that contains the subquery
            // E.g. t0.v3 < (<subquery>) in the above case
            {
                Iterator<ScalarOperator> outerIt = outerConjuncts.iterator();
                while (outerIt.hasNext()) {
                    ScalarOperator outerConjunct = outerIt.next();
                    if (Utils.collect(outerConjunct, ColumnRefOperator.class).stream()
                            .anyMatch(columnRefOperator -> Objects.equals(applyOp.getOutput(), columnRefOperator))) {
                        outerSubqueryConjunct = outerConjunct;
                        outerIt.remove();
                        break;
                    }
                }
            }
            if (outerSubqueryConjunct == null) {
                return false;
            }

            // Third, remove all the conjuncts which only related to the correlated outer table from the outerConjuncts
            // E.g. t0.v2 < 5 in the above case
            {
                Iterator<ScalarOperator> outerIt = outerConjuncts.iterator();
                while (outerIt.hasNext()) {
                    ScalarOperator outerConjunct = outerIt.next();
                    List<ColumnRefOperator> colRefs = Utils.collect(outerConjunct, ColumnRefOperator.class);
                    boolean relatedToOtherTable = false;
                    for (ColumnRefOperator colRef : colRefs) {
                        Column column = context.getColumnRefFactory().getColumn(colRef);
                        if (column == null) {
                            continue;
                        }
                        if (!Objects.equals(column, correlatedOuterTable.getColumn(column.getName()))) {
                            relatedToOtherTable = true;
                            break;
                        }
                    }
                    if (!relatedToOtherTable) {
                        outerIt.remove();
                    }
                }
            }

            // We assume that the right subtree only contains one FilterOperator, for other complex cases, we may
            // support later
            if (rightOps.stream().filter(LogicalFilterOperator.class::isInstance).count() > 1) {
                return false;
            }

            // Forth, check all the conjuncts from the remaining outerConjuncts and subConjuncts are exactly the same
            // E.g. t1.v5 >10 in the above case
            LogicalFilterOperator subFilterOp = rightOps.stream()
                    .filter(LogicalFilterOperator.class::isInstance)
                    .map(Operator::<LogicalFilterOperator>cast)
                    .findFirst()
                    .orElse(null);
            if (subFilterOp == null) {
                return outerConjuncts.isEmpty();
            }
            List<ScalarOperator> subConjuncts = Utils.extractConjuncts(subFilterOp.getPredicate());
            if (outerConjuncts.size() != subConjuncts.size()) {
                return false;
            }
            {
                Iterator<ScalarOperator> outerIt = outerConjuncts.iterator();
                while (outerIt.hasNext()) {
                    ScalarOperator outerConjunct = outerIt.next();
                    Iterator<ScalarOperator> subIt = subConjuncts.iterator();
                    boolean found = false;
                    while (subIt.hasNext()) {
                        ScalarOperator subConjunct = subIt.next();
                        if (PredicateComparator.isIdentical(outerConjunct, subConjunct,
                                context.getColumnRefFactory())) {
                            found = true;
                            outerIt.remove();
                            subIt.remove();
                            break;
                        }
                    }
                    if (!found) {
                        return false;
                    }
                }
            }
            Preconditions.checkState(outerConjuncts.isEmpty());
            Preconditions.checkState(subConjuncts.isEmpty());

            return true;
        }

        private void createWindowOp() {
            outerColumnRefOperators = getAllColumnRefOperators(leftOps);

            Map<ColumnRefOperator, CallOperator> windows = Maps.newHashMap();
            Map<ColumnRefOperator, CallOperator> aggregations = subAggOp.getAggregations();

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregations.entrySet()) {
                ColumnRefOperator columnRefOperator = entry.getKey();
                CallOperator callOp = entry.getValue();

                CallOperator newCallOp = cloneCallOperator(callOp);
                ColumnRefOperator newColumnRefOperator = context.getColumnRefFactory()
                        .create(newCallOp, newCallOp.getType(), newCallOp.isNullable());
                windows.put(newColumnRefOperator, newCallOp);
                peerColumnRefMap.put(columnRefOperator, newColumnRefOperator);
            }

            List<ScalarOperator> partitionByColumns = Lists.newArrayList();
            for (ScalarOperator outerCorrelatedConjunct : outerCorrelatedConjuncts) {
                Preconditions.checkState(outerCorrelatedConjunct instanceof BinaryPredicateOperator);
                // We can take either first or second child of outerCorrelatedConjunct as the partition by expr
                partitionByColumns.add(outerCorrelatedConjunct.getChild(0));
            }

            windowOp = new LogicalWindowOperator.Builder()
                    .setWindowCall(windows)
                    .setPartitionExpressions(partitionByColumns)
                    .setEnforceSortColumns(
                            partitionByColumns.stream()
                                    .map(ScalarOperator::<ColumnRefOperator>cast)
                                    .map(columnRefOperator -> new Ordering(columnRefOperator, true, true))
                                    .collect(Collectors.toList()))
                    .build();
        }

        /**
         * Reform the outer block's predicate into two parts, one contains the subquery conjunct
         * while the others forms another part.
         */
        private void rewriteOuterPredicate() {
            LogicalFilterOperator filterOperator = filter.getOp().cast();
            ScalarOperator predicate = filterOperator.getPredicate();
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            conjuncts.removeIf(conjunct -> Objects.equals(outerSubqueryConjunct, conjunct));
            predicateBeforeWindow = Utils.compoundAnd(conjuncts);

            SubqueryReplaceShuttle visitor = new SubqueryReplaceShuttle(applyOp, peerColumnRefMap);
            predicateAfterWindow = outerSubqueryConjunct.accept(visitor, null);
        }

        private List<OptExpression> getNewExpressionRoot() {
            OptExpression beforeWindowFilter =
                    OptExpression.create(new LogicalFilterOperator(predicateBeforeWindow), apply.inputAt(0));
            OptExpression window = OptExpression.create(windowOp, beforeWindowFilter);
            OptExpression afterWindowFilter =
                    OptExpression.create(new LogicalFilterOperator(predicateAfterWindow), window);

            Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(projectOp.getColumnRefMap());
            windowOp.getWindowCall().keySet()
                    .forEach(columnRefOperator -> newProjectMap.put(columnRefOperator, columnRefOperator));
            subAggOp.getAggregations().keySet()
                    .forEach(newProjectMap::remove);
            newProjectMap.remove(applyOp.getOutput());
            OptExpression newProject = OptExpression.create(new LogicalProjectOperator.Builder()
                    .withOperator(project.getOp().cast())
                    .setColumnRefMap(newProjectMap)
                    .build(), afterWindowFilter);

            return Collections.singletonList(newProject);
        }

        private void collectInLevelOrder(OptExpression root, List<LogicalOperator> collect) {
            Queue<OptExpression> queue = Lists.newLinkedList();
            queue.offer(root);
            while (!queue.isEmpty()) {
                OptExpression top = queue.poll();
                collect.add(top.getOp().cast());
                top.getInputs().forEach(queue::offer);
            }
        }

        private List<Table> getAllTables(List<LogicalOperator> ops) {
            List<Table> tables = Lists.newArrayList();

            List<LogicalScanOperator> scanOps = ops.stream()
                    .filter(LogicalScanOperator.class::isInstance)
                    .map(Operator::<LogicalScanOperator>cast)
                    .collect(Collectors.toList());

            for (LogicalScanOperator scanOp : scanOps) {
                tables.add(scanOp.getTable());
            }

            return tables;
        }

        private Set<ColumnRefOperator> getAllColumnRefOperators(List<LogicalOperator> ops) {
            Set<ColumnRefOperator> columnRefOperators = Sets.newHashSet();

            List<LogicalProjectOperator> projectOps = ops.stream()
                    .filter(LogicalProjectOperator.class::isInstance)
                    .map(Operator::<LogicalProjectOperator>cast)
                    .collect(Collectors.toList());

            for (LogicalProjectOperator projectOp : projectOps) {
                columnRefOperators.addAll(projectOp.getColumnRefMap().keySet());
            }

            return columnRefOperators;
        }

        /**
         * Clone callOperator using the columnRefOperators from the outer block's relation, since the window function
         * comprises columnRefOperators which come from the subquery's relation. And in the meanwhile maintain the
         * columnRefOperator(from subquery relation) -> columnRefOperator(from outer block relation) mapping
         */
        private CallOperator cloneCallOperator(CallOperator callOp) {
            ScalarOperatorCloneShuttle visitor =
                    new ScalarOperatorCloneShuttle(context.getColumnRefFactory(), peerColumnRefMap,
                            rightColumnRefMap, outerColumnRefOperators);
            return callOp.accept(visitor, null).cast();
        }
    }

    /**
     * This comparator used to determine whether two scalarOperator are identical
     * For ColumnRefOperator, we need to check whether the Column it points to is the same
     * For other types of ScalarOperator, just do some normal checks and recursively check its children
     */
    private static final class PredicateComparator extends ScalarOperatorVisitor<Boolean, ScalarOperator> {
        private final ColumnRefFactory columnRefFactory;

        private PredicateComparator(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        public static boolean isIdentical(ScalarOperator expected, ScalarOperator target,
                                          ColumnRefFactory columnRefFactory) {
            PredicateComparator visitor = new PredicateComparator(columnRefFactory);
            return expected.accept(visitor, target);
        }

        private boolean isClassMismatch(ScalarOperator expected, ScalarOperator target) {
            return !expected.getClass().equals(target.getClass());
        }

        private boolean compare(ScalarOperator scalarOperator, ScalarOperator peer) {
            if (scalarOperator.getChildren().size() != peer.getChildren().size()) {
                return false;
            }
            for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
                if (!scalarOperator.getChild(i).accept(this, peer.getChild(i))) {
                    return false;
                }
            }
            return true;
        }

        private boolean compareBinaryOperator(BinaryPredicateOperator predicate, BinaryPredicateOperator peer) {
            if (!Objects.equals(predicate.getBinaryType(), peer.getBinaryType())) {
                return false;
            }
            if (compareChildrenOfBinaryOperator(predicate, peer)) {
                return true;
            }
            if (BinaryPredicateOperator.BinaryType.EQ.equals(predicate.getBinaryType())) {
                // For equal, we need to compare children in another order
                return compareChildrenOfBinaryOperator(predicate, peer)
                        || compareChildrenOfBinaryOperator(predicate, peer.commutative());
            } else {
                return compareChildrenOfBinaryOperator(predicate, peer);
            }
        }

        private boolean compareChildrenOfBinaryOperator(BinaryPredicateOperator predicate,
                                                        BinaryPredicateOperator peer) {
            return predicate.getChild(0).accept(this, peer.getChild(0))
                    && predicate.getChild(1).accept(this, peer.getChild(1));
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, ScalarOperator peer) {
            return false;
        }

        @Override
        public Boolean visitConstant(ConstantOperator literal, ScalarOperator peer) {
            return Objects.equals(literal, peer);
        }

        @Override
        public Boolean visitVariableReference(ColumnRefOperator variable, ScalarOperator peer) {
            if (isClassMismatch(variable, peer)) {
                return false;
            }
            Column column1 = columnRefFactory.getColumn(variable);
            Column column2 = columnRefFactory.getColumn((ColumnRefOperator) peer);
            return Objects.equals(column1, column2);
        }

        @Override
        public Boolean visitCall(CallOperator call, ScalarOperator peer) {
            if (isClassMismatch(call, peer)) {
                return false;
            }
            CallOperator peerCall = peer.cast();
            if (!Objects.equals(call.getFunction(), peerCall.getFunction())) {
                return false;
            }
            return compare(call, peerCall);
        }

        @Override
        public Boolean visitBetweenPredicate(BetweenPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }

            BinaryPredicateOperator peerPredicate = peer.cast();
            return compareBinaryOperator(predicate, peerPredicate);
        }

        @Override
        public Boolean visitCompoundPredicate(CompoundPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitExistsPredicate(ExistsPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitInPredicate(InPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitIsNullPredicate(IsNullPredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitLikePredicateOperator(LikePredicateOperator predicate, ScalarOperator peer) {
            if (isClassMismatch(predicate, peer)) {
                return false;
            }
            return compare(predicate, peer);
        }

        @Override
        public Boolean visitCastOperator(CastOperator operator, ScalarOperator peer) {
            return super.visitCastOperator(operator, peer);
        }

        @Override
        public Boolean visitCaseWhenOperator(CaseWhenOperator operator, ScalarOperator peer) {
            return super.visitCaseWhenOperator(operator, peer);
        }
    }

    /**
     * For every ColumnRefOperator in subquery, we need to replace it with the corresponding peer in the outer block
     * For example,
     * select * from t0, t1
     * where t0.v1 = t1.v4
     * and t0.v2 < 5
     * and t1.v5 > 10
     * and t0.v3 < (select max(v6 * v5) from t1 where t0.v1 = t1.v4 and t1.v5 >10 )
     * <p>
     * When cloning argument of aggregate function max, i.e. v6 * v5, v5 and v6 come from the subquery's relation
     * we need to find the corresponding v5 and v6 from outer block's relation
     */
    private static final class ScalarOperatorCloneShuttle extends BaseScalarOperatorShuttle {
        private final ColumnRefFactory columnRefFactory;
        private final Map<ColumnRefOperator, ColumnRefOperator> columnRefMapping;
        private final Map<ColumnRefOperator, ScalarOperator> rightColumnRefMap;
        private final Set<ColumnRefOperator> outerColumnRefOperators;

        public ScalarOperatorCloneShuttle(ColumnRefFactory columnRefFactory,
                                          Map<ColumnRefOperator, ColumnRefOperator> columnRefMapping,
                                          Map<ColumnRefOperator, ScalarOperator> rightColumnRefMap,
                                          Set<ColumnRefOperator> outerColumnRefOperators) {
            this.columnRefFactory = columnRefFactory;
            this.columnRefMapping = columnRefMapping;
            this.rightColumnRefMap = rightColumnRefMap;
            this.outerColumnRefOperators = outerColumnRefOperators;
        }

        public ScalarOperator clone(ScalarOperator scalarOperator) {
            return scalarOperator.accept(this, null);
        }

        public List<ScalarOperator> cloneChildren(ScalarOperator scalarOperator) {
            return scalarOperator.getChildren().stream()
                    .map(this::clone)
                    .collect(Collectors.toList());
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            // Throw exception for all unexpected situations, which will be caught in the transformer
            throw new UnsupportedOperationException();
        }

        @Override
        public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
            return literal;
        }

        @Override
        public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
            return new BetweenPredicateOperator(predicate.isNotBetween(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            return new BinaryPredicateOperator(predicate.getBinaryType(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            return new CompoundPredicateOperator(predicate.getCompoundType(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
            return new ExistsPredicateOperator(predicate.isNotExists(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
            return new InPredicateOperator(predicate.isNotIn(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return new IsNullPredicateOperator(predicate.isNotNull(), clone(predicate.getChild(0)));
        }

        @Override
        public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            return new LikePredicateOperator(predicate.getLikeType(), cloneChildren(predicate));
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            return new CallOperator(call.getFnName(), call.getType(), cloneChildren(call),
                    call.getFunction(), call.isDistinct());
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
            return new CastOperator(operator.getType(), clone(operator.getChild(0)), operator.isImplicit());
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            List<ScalarOperator> clonedWhenThenClauses = Lists.newArrayList();
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                clonedWhenThenClauses.add(clone(operator.getWhenClause(i)));
            }
            return new CaseWhenOperator(operator.getType(), clone(operator.getCaseClause()),
                    clone(operator.getElseClause()), clonedWhenThenClauses);
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
            Column column = columnRefFactory.getColumn(columnRefOperator);
            if (column != null) {
                Map<Column, List<ColumnRefOperator>> columnToColumnRefs = Maps.newHashMap();
                Map<ColumnRefOperator, Column> columnRefToColumns = columnRefFactory.getColumnRefToColumns();
                for (Map.Entry<ColumnRefOperator, Column> entry : columnRefToColumns.entrySet()) {
                    ColumnRefOperator key = entry.getKey();
                    Column value = entry.getValue();
                    if (!columnToColumnRefs.containsKey(value)) {
                        columnToColumnRefs.put(value, Lists.newArrayList());
                    }
                    columnToColumnRefs.get(value).add(key);
                }
                Preconditions.checkNotNull(column);

                List<ColumnRefOperator> columnRefOperators = columnToColumnRefs.get(column);
                for (ColumnRefOperator alternative : columnRefOperators) {
                    if (!Objects.equals(alternative, columnRefOperator)) {
                        // The alternative must exist in the left subtree of apply
                        if (outerColumnRefOperators.contains(alternative)) {
                            columnRefMapping.put(columnRefOperator, alternative);
                            return alternative;
                        }
                    }
                }
            } else {
                ScalarOperator scalarOperator = rightColumnRefMap.get(columnRefOperator);
                Preconditions.checkNotNull(scalarOperator);
                Preconditions.checkState(!(scalarOperator instanceof ColumnRefOperator));
                return scalarOperator.accept(this, null);
            }

            return columnRefOperator;
        }
    }

    /**
     * Replace all the columnRefOperators of the subquery predicate with the corresponding peer
     * For example
     * select * from t0, t1
     * where t0.v1 = t1.v4
     * and t0.v2 < 5
     * and t1.v5 > 10
     * and t0.v3 < (select max(v6) from t1 where t0.v1 = t1.v4 and t1.v5 >10 )
     * <p>
     * t0.v3 < agg_max(v6) should be replaced with t0.v3 < win_agg(v6)
     */
    private static final class SubqueryReplaceShuttle extends BaseScalarOperatorShuttle {

        private final LogicalApplyOperator applyOp;
        private final Map<ColumnRefOperator, ColumnRefOperator> columnRefMapping;

        public SubqueryReplaceShuttle(LogicalApplyOperator applyOp,
                                      Map<ColumnRefOperator, ColumnRefOperator> columnRefMapping) {
            this.applyOp = applyOp;
            this.columnRefMapping = columnRefMapping;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
            if (Objects.equals(columnRefOperator, applyOp.getOutput())) {
                return applyOp.getSubqueryOperator().accept(this, null);
            } else {
                ColumnRefOperator replaceColumnRefOperator = columnRefMapping.get(columnRefOperator);
                if (replaceColumnRefOperator != null) {
                    return replaceColumnRefOperator;
                }
                return columnRefOperator;
            }
        }
    }
}