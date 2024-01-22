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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PruneUKFKJoinRule extends TransformationRule {
    public PruneUKFKJoinRule() {
        super(RuleType.TF_PRUNE_UKFK_JOIN, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF,
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return ConnectContext.get().getSessionVariable().isEnableUKFKOpt();
    }

    @Override
    public List<OptExpression> transform(OptExpression projectOpt, OptimizerContext context) {
        LogicalProjectOperator projectOp = projectOpt.getOp().cast();
        OptExpression joinOpt = projectOpt.inputAt(0);

        UKFKConstraintsCollector.collectColumnConstraints(joinOpt);

        LogicalJoinOperator joinOp = joinOpt.getOp().cast();
        JoinOperator joinType = joinOp.getJoinType();
        UKFKConstraints constraints = joinOpt.getConstraints();
        UKFKConstraints.JoinProperty property = constraints.getJoinProperty();

        if (property == null) {
            return Lists.newArrayList();
        }

        if (joinType.isInnerJoin() ||
                ((joinType.isLeftOuterJoin() || joinType.isLeftSemiJoin()) && !property.isLeftUK) ||
                ((joinType.isRightOuterJoin() || joinType.isRightSemiJoin()) && property.isLeftUK)) {
            int ukChildIdx = property.isLeftUK ? 0 : 1;
            OptExpression ukChildOpt = joinOpt.inputAt(ukChildIdx);
            OptExpression fkChildOpt = joinOpt.inputAt(1 - ukChildIdx);

            boolean notOuterJoin = !joinType.isOuterJoin();

            // The uk side child can be pruned if the following conditions are met:
            // For Outer Join:
            //   1. uk is the only column from uk table that is used by join output.
            //   2. uk is the only column from uk table that is used by join.
            //   3. uk is the only column from uk table that is used by its children.
            // For Inner and Semi Join:
            //   1. uk is the only column from uk table that is used by join output.
            //   2. no column from uk table can be used by join.
            //   3. no column from uk table can be used by its children.
            if (!isNonUKTableColumnUsedByJoinOutput(property, joinOpt, ukChildOpt) &&
                    !isNonUKTableColumnUsedByJoinItself(property, joinOpt, ukChildOpt, notOuterJoin) &&
                    !isNonUKTableColumnUsedByUKSideChildren(property, ukChildOpt, notOuterJoin)) {

                OptExpression filterOpt = buildFilterOpt(property, ukChildOpt, fkChildOpt, notOuterJoin);
                OptExpression newProjectOpt = buildProjectOpt(property, projectOp, filterOpt);

                return Lists.newArrayList(newProjectOpt);
            }
        }

        return Lists.newArrayList();
    }

    private boolean isNonUKTableColumnUsedByJoinOutput(UKFKConstraints.JoinProperty property,
                                                       OptExpression joinOpt, OptExpression ukChildOpt) {
        RowOutputInfo ukRowOutputInfo = ukChildOpt.getRowOutputInfo();
        ColumnRefSet ukTableColumnRefs = ukRowOutputInfo.getOutputColumnRefSet();
        ukTableColumnRefs.except(Collections.singletonList(property.ukColumnRef));

        RowOutputInfo joinRowOutputInfo = joinOpt.getRowOutputInfo();
        ColumnRefSet joinOutputColumnRefs = joinRowOutputInfo.getOutputColumnRefSet();

        return joinOutputColumnRefs.containsAny(ukTableColumnRefs);
    }

    private boolean isNonUKTableColumnUsedByJoinItself(UKFKConstraints.JoinProperty property,
                                                       OptExpression joinOpt, OptExpression ukChildOpt,
                                                       boolean exceptUK) {
        RowOutputInfo ukRowOutputInfo = ukChildOpt.getRowOutputInfo();
        ColumnRefSet ukTableColumnRefs = ukRowOutputInfo.getOutputColumnRefSet();
        if (exceptUK) {
            ukTableColumnRefs.except(Collections.singletonList(property.ukColumnRef));
        }

        LogicalJoinOperator joinOp = joinOpt.getOp().cast();
        ColumnRefSet joinUsedColumns = new ColumnRefSet();
        if (joinOp.getOnPredicate() != null) {
            Utils.extractConjuncts(joinOp.getOnPredicate())
                    .stream().filter(p -> !Objects.equals(property.predicate, p))
                    .forEach(p -> joinUsedColumns.union(p.getUsedColumns()));
        }

        return joinUsedColumns.containsAny(ukTableColumnRefs);
    }

    private boolean isNonUKTableColumnUsedByUKSideChildren(UKFKConstraints.JoinProperty property,
                                                           OptExpression ukChildOpt,
                                                           boolean exceptUK) {
        ColumnRefSet ukTableColumnRefs = property.ukConstraint.nonUKColumnRefs;
        if (!exceptUK) {
            ukTableColumnRefs.union(Collections.singletonList(property.ukColumnRef));
        }

        ColumnRefSet childrenUsedColumns = UsedColumnRefCollector.collect(ukChildOpt);

        return childrenUsedColumns.containsAny(ukTableColumnRefs);
    }

    private ScalarOperator collectUKPredicate(OptExpression ukChildOpt) {
        List<ScalarOperator> predicates = Lists.newArrayList();
        while (ukChildOpt != null) {
            if (ukChildOpt.getOp().getPredicate() != null) {
                predicates.add(ukChildOpt.getOp().getPredicate());
            }
            if (ukChildOpt.arity() != 1) {
                break;
            }
            ukChildOpt = ukChildOpt.inputAt(0);
        }

        return Utils.compoundAnd(predicates);
    }

    private OptExpression buildFilterOpt(UKFKConstraints.JoinProperty property,
                                         OptExpression ukChildOpt,
                                         OptExpression fkChildOpt,
                                         boolean addIsNullPredicate) {
        ScalarOperator ukPredicate = collectUKPredicate(ukChildOpt);
        if (ukPredicate != null) {
            Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
            replaceMap.put(property.ukColumnRef, property.fkColumnRef);
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap, true);
            ukPredicate = rewriter.rewrite(ukPredicate);
        }
        if (addIsNullPredicate) {
            IsNullPredicateOperator isNullPredicate = new IsNullPredicateOperator(true, property.fkColumnRef);
            ukPredicate = Utils.compoundAnd(ukPredicate, isNullPredicate);
        }
        if (ukPredicate == null) {
            return fkChildOpt;
        }
        LogicalFilterOperator filterOp = new LogicalFilterOperator(ukPredicate);
        return OptExpression.create(filterOp, fkChildOpt);
    }

    private OptExpression buildProjectOpt(UKFKConstraints.JoinProperty property, LogicalProjectOperator projectOp,
                                          OptExpression filterOpt) {
        Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
        replaceMap.put(property.ukColumnRef, property.fkColumnRef);
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap, true);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOp.getColumnRefMap().entrySet()) {
            if (entry.getValue().getUsedColumns().contains(property.ukColumnRef)) {
                projectMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            } else {
                projectMap.put(entry.getKey(), entry.getValue());
            }
        }

        LogicalProjectOperator newProjectOp = new LogicalProjectOperator.Builder()
                .withOperator(projectOp)
                .setColumnRefMap(projectMap)
                .build();
        return OptExpression.create(newProjectOp, filterOpt);
    }

    private static class UsedColumnRefCollector extends OptExpressionVisitor<Void, ColumnRefSet> {

        private static ColumnRefSet collect(OptExpression root) {
            ColumnRefSet used = new ColumnRefSet();
            UsedColumnRefCollector collector = new UsedColumnRefCollector();
            collector.visit(root, used);
            return used;
        }

        @Override
        public Void visit(OptExpression optExpression, ColumnRefSet context) {
            LogicalOperator op = optExpression.getOp().cast();
            if (op.getPredicate() != null) {
                context.union(op.getPredicate().getUsedColumns());
            }
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }

            return null;
        }
    }
}