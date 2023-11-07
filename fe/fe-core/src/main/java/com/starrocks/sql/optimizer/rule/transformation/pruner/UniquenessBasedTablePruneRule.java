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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// UniquenessBasedTablePruneRule is dedicated to prune right hand side of left outer join
// or left hand side of right outer join based on the fact that join columns of the pruned side
// are unique and its columns are used in output columns as the join.
// So this rule is not required that foreign key constraint to bridge two tables joined together,
// and it can not be apply to inner join and does not have ability of column equivalence inferring.
// for an example:
//```SQL
// select l_tax from lineitem l left join orders o on l.l_orderkey = o.o_orderkey
//```
// orders have unique key constraint on o_orderkey and only l_tax is used, so orders can be pruned,
// so the SQL optimized as follows:
//```SQL
//select l_tax from lineitem.
//```
// The uniqueness of join columns are not only inferred from uniqueness constraints, but also can be inferred from
// the fact group-by columns of group-by aggregation are unique.
// for an example:
//```SQL
// select l.l_tax
// from lineitem l left join
// (select l_orderkey from lineitem group by l_orderkey) t on l_orderkey = t.l_orderkey
//```
// t.l_orderkey is unique and only l.l_tax is output, so the SQL can be optimized as follows:
//```
// select l_tax from lineitem
//```
public class UniquenessBasedTablePruneRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        Collector collector = new Collector();
        collector.collect(root);
        Pruner pruner = new Pruner(collector.getCandidateJoins());
        return pruner.prune(root);
    }

    private static class Collector extends OptExpressionVisitor<Boolean, Void> {
        private final Map<OptExpression, List<ColumnRefSet>> optToUniqueKeys = Maps.newHashMap();
        private final Set<OptExpression> candidateJoins = Sets.newHashSet();

        public Set<OptExpression> getCandidateJoins() {
            return candidateJoins;
        }

        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            optToUniqueKeys.put(optExpression, Collections.emptyList());
            return false;
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            if (!(optExpression.getOp() instanceof LogicalOlapScanOperator)) {
                return visit(optExpression, context);
            }
            LogicalOlapScanOperator scanOp = optExpression.getOp().cast();
            OlapTable table = (OlapTable) scanOp.getTable();
            if (!table.hasUniqueConstraints()) {
                return visit(optExpression, context);
            }
            Map<String, ColumnRefOperator> nameToColRefMap = scanOp.getColumnNameToColRefMap();
            List<ColumnRefSet> uniqueKeys = table.getUniqueConstraints().stream().map(uc ->
                    new ColumnRefSet(uc.getUniqueColumns().stream().map(nameToColRefMap::get)
                            .collect(Collectors.toList()))).collect(Collectors.toList());
            uniqueKeys = propagateThroughProjection(optExpression, uniqueKeys);
            optToUniqueKeys.put(optExpression, uniqueKeys);
            return true;
        }

        @Override
        public Boolean visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator aggOp = optExpression.getOp().cast();
            if (aggOp.getGroupingKeys().isEmpty()) {
                return visit(optExpression, context);
            }
            ColumnRefSet groupingKeys = new ColumnRefSet(aggOp.getGroupingKeys());
            List<ColumnRefSet> uniqueKeys =
                    optToUniqueKeys.getOrDefault(optExpression.inputAt(0), Collections.emptyList());
            uniqueKeys = uniqueKeys.stream().filter(groupingKeys::containsAll).collect(Collectors.toList());
            uniqueKeys = uniqueKeys.isEmpty() ? Collections.singletonList(groupingKeys) : uniqueKeys;
            uniqueKeys = propagateThroughProjection(optExpression, uniqueKeys);
            optToUniqueKeys.put(optExpression, uniqueKeys);
            return false;
        }

        public List<ColumnRefSet> propagateThroughProjection(OptExpression optExpression,
                                                             List<ColumnRefSet> uniqueKeys) {
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = Collections.emptyMap();
            if (optExpression.getOp() instanceof LogicalProjectOperator) {
                LogicalProjectOperator projectOp = optExpression.getOp().cast();
                columnRefMap = projectOp.getColumnRefMap();
            } else if (optExpression.getOp().getProjection() != null) {
                columnRefMap = optExpression.getOp().getProjection().getColumnRefMap();
            }

            if (columnRefMap.isEmpty()) {
                return uniqueKeys;
            }
            List<ColumnRefOperator> colRefs = columnRefMap.entrySet().stream()
                    .filter(e -> e.getValue().isColumnRef())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            ColumnRefSet columnRefSet = new ColumnRefSet(colRefs);
            return uniqueKeys.stream().filter(columnRefSet::containsAll).collect(Collectors.toList());
        }

        Boolean propagateBottomUp(OptExpression optExpression, int childIdx) {
            OptExpression child = optExpression.inputAt(childIdx);
            List<ColumnRefSet> uniqueKeys = optToUniqueKeys.getOrDefault(child, Collections.emptyList());
            uniqueKeys = propagateThroughProjection(optExpression, uniqueKeys);
            if (!uniqueKeys.isEmpty()) {
                optToUniqueKeys.put(optExpression, uniqueKeys);
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            return propagateBottomUp(optExpression, 0);
        }

        @Override
        public Boolean visitLogicalCTEConsume(OptExpression optExpression, Void context) {
            // In some cases(TPC-DS q45), we have high certainty that a query can benefit from CTE optimization;
            // then LogicalCTEConsumeOperators in its plan has no children to avoid CTE inlining or other
            // respective optimization.
            if (optExpression.getInputs().isEmpty()) {
                return false;
            }
            return propagateBottomUp(optExpression, 0);
        }

        @Override
        public Boolean visitLogicalCTEAnchor(OptExpression optExpression, Void context) {
            return propagateBottomUp(optExpression, 1);
        }

        @Override
        public Boolean visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOp = optExpression.getOp().cast();
            JoinOperator joinType = joinOp.getJoinType();
            OptExpression lhs = optExpression.inputAt(0);
            OptExpression rhs = optExpression.inputAt(1);
            List<ColumnRefSet> lhsUniqueKeys = optToUniqueKeys.getOrDefault(lhs, Collections.emptyList());
            List<ColumnRefSet> rhsUniqueKeys = optToUniqueKeys.getOrDefault(rhs, Collections.emptyList());
            if (lhsUniqueKeys.isEmpty() && rhsUniqueKeys.isEmpty()) {
                return visit(optExpression, context);
            }
            // unmatched rows of lhs are duplicate for rhs, vice versa
            if (joinType.isFullOuterJoin()) {
                return visit(optExpression, context);
            }
            // left anti/semi join output subset of lhs, duplication factor is not changed,
            // so uniqueness can propagate from lhs
            if (joinType.isLeftSemiAntiJoin() && !lhsUniqueKeys.isEmpty()) {
                optToUniqueKeys.put(optExpression, propagateThroughProjection(optExpression, lhsUniqueKeys));
                return true;
            }
            // right anti/semi join is mirror symmetric to left anti/semi join
            if (joinType.isRightSemiAntiJoin() && !rhsUniqueKeys.isEmpty()) {
                optToUniqueKeys.put(optExpression, propagateThroughProjection(optExpression, rhsUniqueKeys));
                return !rhsUniqueKeys.isEmpty();
            }

            Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> onPredicates =
                    JoinHelper.separateEqualPredicatesFromOthers(optExpression);
            List<BinaryPredicateOperator> eqJoinOnPredicates = onPredicates.first;
            List<ScalarOperator> otherJoinOnPredicates = onPredicates.second;
            if (eqJoinOnPredicates.isEmpty()) {
                return visit(optExpression, context);
            }
            ColumnRefSet lhsJoinColRefSet = new ColumnRefSet();
            ColumnRefSet rhsJoinColRefSet = new ColumnRefSet();
            for (BinaryPredicateOperator eqPredicate : eqJoinOnPredicates) {
                lhsJoinColRefSet.union((ColumnRefOperator) eqPredicate.getChild(0));
                rhsJoinColRefSet.union((ColumnRefOperator) eqPredicate.getChild(1));
            }
            lhsUniqueKeys = lhsUniqueKeys.stream().filter(lhsJoinColRefSet::containsAll).collect(Collectors.toList());
            rhsUniqueKeys = rhsUniqueKeys.stream().filter(rhsJoinColRefSet::containsAll).collect(Collectors.toList());
            if (lhsUniqueKeys.isEmpty() && rhsUniqueKeys.isEmpty()) {
                return visit(optExpression, context);
            }

            // inner join has unique keys, only when both side have unique keys and join on these unique keys.
            if (joinType.isInnerJoin() && !lhsUniqueKeys.isEmpty() && !rhsUniqueKeys.isEmpty()) {
                List<ColumnRefSet> uniqueKeys = Lists.newArrayList();
                uniqueKeys.addAll(lhsUniqueKeys);
                uniqueKeys.addAll(rhsUniqueKeys);
                optToUniqueKeys.put(optExpression, propagateThroughProjection(optExpression, uniqueKeys));
                return true;
            }

            boolean hasNoOtherPredicates = otherJoinOnPredicates.isEmpty();
            // lhs' uniqueness propagates upwards only if rhs' join key is subset of rhs' unique key
            if (joinType.isLeftOuterJoin() && !rhsUniqueKeys.isEmpty()) {
                optToUniqueKeys.put(optExpression, propagateThroughProjection(optExpression, lhsUniqueKeys));
                // other join on predicates may reference ColumnRefs of both side, so the join is unprunable.
                if (hasNoOtherPredicates) {
                    candidateJoins.add(optExpression);
                }
                return true;
            }
            // right join is mirror symmetric to left join
            if (joinType.isRightOuterJoin() && !lhsUniqueKeys.isEmpty()) {
                optToUniqueKeys.put(optExpression, propagateThroughProjection(optExpression, rhsUniqueKeys));
                if (hasNoOtherPredicates) {
                    candidateJoins.add(optExpression);
                }
                return true;
            }
            return visit(optExpression, context);
        }

        void collect(OptExpression root) {
            root.getInputs().forEach(this::collect);
            root.getOp().accept(this, root, null);
        }
    }

    public static class Pruner extends OptExpressionVisitor<Optional<OptExpression>, ColumnRefSet> {
        Set<OptExpression> candidateJoins;

        Pruner(Set<OptExpression> candidateJoins) {
            this.candidateJoins = candidateJoins;
        }

        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, ColumnRefSet context) {
            return Optional.empty();
        }
        
        private Optional<OptExpression> pruneJoin(OptExpression optExpression, OptExpression retainedChd,
                                                  OptExpression prunedChd) {
            Operator joinOp = optExpression.getOp();
            Operator retainedOp = retainedChd.getOp();
            ColumnRefSet requiredColRefSet = new ColumnRefSet();
            optExpression.getRowOutputInfo().getColumnRefMap().values().stream()
                    .map(ScalarOperator::getUsedColumns).forEach(requiredColRefSet::union);
            Optional.ofNullable(joinOp.getPredicate())
                    .map(ScalarOperator::getUsedColumns).ifPresent(requiredColRefSet::union);

            ColumnRefSet outputColRefSet = prunedChd.getRowOutputInfo().getOutputColumnRefSet();
            // Only if the output columns of the pruned child is not used by join operator, then
            // pruned child can be pruned really.
            if (outputColRefSet.containsAny(requiredColRefSet)) {
                return Optional.empty();
            }
            // cook new {ColumnRefMap, Predicates, Limit} from retained child's and the join operator's.
            Map<ColumnRefOperator, ScalarOperator> joinColRefMap =
                    Optional.ofNullable(optExpression.getOp().getProjection())
                            .map(Projection::getColumnRefMap).orElse(Collections.emptyMap());

            Map<ColumnRefOperator, ScalarOperator> retainedChdColRefMap =
                    Optional.ofNullable(retainedChd.getOp().getProjection())
                            .map(Projection::getColumnRefMap).orElse(Collections.emptyMap());

            ReplaceColumnRefRewriter replacer = new ReplaceColumnRefRewriter(retainedChdColRefMap, false);

            Map<ColumnRefOperator, ScalarOperator> newColRefMap = Maps.newHashMap();
            joinColRefMap.forEach((k, v) -> newColRefMap.put(k, replacer.rewrite(v)));

            ScalarOperator joinPredicate =
                    Optional.ofNullable(joinOp.getPredicate()).map(replacer::rewrite).orElse(null);
            ScalarOperator newPredicate = Utils.compoundAnd(joinPredicate, retainedOp.getPredicate());
            newPredicate = Utils.compoundAnd(Utils.extractConjuncts(newPredicate));
            long newLimit = retainedOp.hasLimit() ? retainedOp.getLimit() : joinOp.getLimit();
            Operator newOp = OperatorBuilderFactory.build(retainedOp).withOperator(retainedOp)
                    .setProjection(new Projection(newColRefMap))
                    .setPredicate(newPredicate)
                    .setLimit(newLimit)
                    .build();
            return Optional.of(OptExpression.create(newOp, retainedChd.getInputs()));
        }

        @Override
        public Optional<OptExpression> visitLogicalJoin(OptExpression optExpression, ColumnRefSet context) {
            if (!candidateJoins.contains(optExpression) || optExpression.getOp().getProjection() == null) {
                return Optional.empty();
            }
            LogicalJoinOperator joinOp = optExpression.getOp().cast();
            JoinOperator joinType = joinOp.getJoinType();
            Preconditions.checkArgument(joinType.isLeftOuterJoin() || joinType.isRightOuterJoin());
            OptExpression retainedChild = optExpression.inputAt(joinType.isLeftOuterJoin() ? 0 : 1);
            OptExpression prunedChild = optExpression.inputAt(joinType.isLeftOuterJoin() ? 1 : 0);
            return pruneJoin(optExpression, retainedChild, prunedChild);
        }

        OptExpression prune(OptExpression root) {
            for (int i = 0; i < root.getInputs().size(); ++i) {
                root.setChild(i, prune(root.inputAt(i)));
            }
            return root.getOp().accept(this, root, null).orElse(root);
        }
    }
}
