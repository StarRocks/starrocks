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
package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.LocalExchangerType;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.RoundRobinDistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/* rewrite the tree top down
 * if one shuffle join op is decided as skew, rewrite it as below and do not visit its children
 *        shuffle join                           union
 *       /         \     --->            /                   \
 *  exchange    exchange           shuffle join        broadcast join
 *     |            |                   /       \           /       \
 *   child1      child2           exchange  exchange  ex(random)  ex(broadcast)
 *                                   |            \      /          /
 *                                   |             \   /          /
 *                                   |             / \          /
 *                                   |           /    \        /
 *                                   |         /       \     /
 *                                  child1            child2
 */
public class SkewShuffleJoinEliminationRule implements TreeRewriteRule {
    private AtomicInteger uniqueSplitId;

    private SkewShuffleJoinEliminationVisitor handler = null;

    public SkewShuffleJoinEliminationRule() {
        this.uniqueSplitId = new AtomicInteger();
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        handler = new SkewShuffleJoinEliminationVisitor(taskContext.getOptimizerContext().getColumnRefFactory());
        // root's parentRequireEmpty = true
        return root.getOp().accept(handler, root, true);
    }

    private class SkewShuffleJoinEliminationVisitor extends OptExpressionVisitor<OptExpression, Boolean> {
        private ColumnRefFactory columnRefFactory;

        public SkewShuffleJoinEliminationVisitor(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Boolean parentRequireEmpty) {
            // default: parent doesn't require empty
            return visitChild(optExpr, false);
        }

        // visit all children of the given opt expression with the given parentRequireEmpty
        private OptExpression visitChild(OptExpression opt, Boolean parentRequireEmpty) {
            for (int idx = 0; idx < opt.arity(); ++idx) {
                OptExpression child = opt.inputAt(idx);
                opt.setChild(idx, child.getOp().accept(this, child, parentRequireEmpty));
            }
            return opt;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression opt, Boolean parentRequireEmpty) {
            PhysicalHashJoinOperator originalShuffleJoinOperator = (PhysicalHashJoinOperator) opt.getOp();
            Projection projectionOnJoin = originalShuffleJoinOperator.getProjection();

            boolean requireEmptyForChild = isBroadCastJoin(opt);
            // doesn't support cross join and right join
            if (originalShuffleJoinOperator.getJoinType().isCrossJoin() ||
                    originalShuffleJoinOperator.getJoinType().isRightJoin()) {
                return visitChild(opt, requireEmptyForChild);
            }

            // right now only support shuffle join
            if (!isShuffleJoin(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            if (!isSkew(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            // find skew columns, both can be warped with cast if column type mismatch
            Pair<ColumnRefOperator, ColumnRefOperator> skewColumns = findSkewColumns(opt);
            ColumnRefOperator leftSkewColumn = skewColumns.first;
            ColumnRefOperator rightSkewColumn = skewColumns.second;
            if (leftSkewColumn == null || rightSkewColumn == null) {
                return opt;
            }

            OptExpression leftExchangeOptExp = opt.inputAt(0);
            PhysicalDistributionOperator leftExchangeOp = (PhysicalDistributionOperator) leftExchangeOptExp.getOp();

            OptExpression rightExchangeOptExp = opt.inputAt(1);
            PhysicalDistributionOperator rightExchangeOp = (PhysicalDistributionOperator) rightExchangeOptExp.getOp();

            PhysicalSplitProduceOperator leftSplitProduceOperator =
                    new PhysicalSplitProduceOperator(uniqueSplitId.getAndIncrement());
            OptExpression leftSplitProduceOptExp = OptExpression.builder()
                    .setOp(leftSplitProduceOperator)
                    .setInputs(leftExchangeOptExp.getInputs())
                    .setLogicalProperty(leftExchangeOptExp.getLogicalProperty())
                    .setStatistics(leftExchangeOptExp.getStatistics())
                    .setCost(leftExchangeOptExp.getCost()).build();

            PhysicalSplitProduceOperator rightSplitProduceOperator =
                    new PhysicalSplitProduceOperator(uniqueSplitId.getAndIncrement());
            OptExpression rightSplitProduceOptExp = OptExpression.builder()
                    .setOp(rightSplitProduceOperator)
                    .setInputs(rightExchangeOptExp.getInputs())
                    .setLogicalProperty(rightExchangeOptExp.getLogicalProperty())
                    .setStatistics(rightExchangeOptExp.getStatistics())
                    .setCost(rightExchangeOptExp.getCost()).build();

            List<ScalarOperator> inPredicateParamsForLeftTable = new ArrayList<>();
            inPredicateParamsForLeftTable.add(leftSkewColumn);
            inPredicateParamsForLeftTable.addAll(originalShuffleJoinOperator.getSkewValues());

            List<ScalarOperator> inPredicateParamsForRightTable = new ArrayList<>();
            inPredicateParamsForRightTable.add(rightSkewColumn);
            inPredicateParamsForRightTable.addAll(originalShuffleJoinOperator.getSkewValues());

            ScalarOperator inSkewPredicateForLeftTable = new InPredicateOperator(false, inPredicateParamsForLeftTable);
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            inSkewPredicateForLeftTable = rewriter.rewrite(inSkewPredicateForLeftTable,
                    ImmutableList.of(new ImplicitCastRule(), new ReduceCastRule()));
            ScalarOperator notInSkewPredicateForLeftTable =
                    new InPredicateOperator(true, inSkewPredicateForLeftTable.getChildren());

            ScalarOperator inSkewPredicateForRightTable =
                    new InPredicateOperator(false, inPredicateParamsForRightTable);
            rewriter = new ScalarOperatorRewriter();
            inSkewPredicateForRightTable = rewriter.rewrite(inSkewPredicateForRightTable,
                    ImmutableList.of(new ImplicitCastRule(), new ReduceCastRule()));
            ScalarOperator notInSkewPredicateForRightTable =
                    new InPredicateOperator(true, inSkewPredicateForRightTable.getChildren());

            Map<ColumnRefOperator, ColumnRefOperator> leftSplitOutputColumnRefMap =
                    generateOutputColumnRefMap(
                            leftExchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory));
            Map<ColumnRefOperator, ColumnRefOperator> rightSplitOutputColumnRefMap =
                    generateOutputColumnRefMap(
                            rightExchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory));

            PhysicalSplitConsumeOperator leftSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(),
                            notInSkewPredicateForLeftTable,
                            leftExchangeOp.getDistributionSpec(), leftSplitOutputColumnRefMap);

            PhysicalSplitConsumeOperator leftSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(), inSkewPredicateForLeftTable,
                            new RoundRobinDistributionSpec(), leftSplitOutputColumnRefMap);

            PhysicalSplitConsumeOperator rightSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(),
                            notInSkewPredicateForRightTable,
                            rightExchangeOp.getDistributionSpec(), rightSplitOutputColumnRefMap);

            PhysicalSplitConsumeOperator rightSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(),
                            inSkewPredicateForRightTable,
                            DistributionSpec.createReplicatedDistributionSpec(), rightSplitOutputColumnRefMap);

            // newShuffleJoinOpt and newBroadcastJoinOpt are the same as the originalShuffleJoinOperator, but project is moved to mergeOpeartor
            PhysicalHashJoinOperator newShuffleJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), projectionOnJoin,
                    originalShuffleJoinOperator.getSkewColumn(), originalShuffleJoinOperator.getSkewValues());

            PhysicalHashJoinOperator newBroadcastJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), projectionOnJoin,
                    originalShuffleJoinOperator.getSkewColumn(), originalShuffleJoinOperator.getSkewValues());
            // we have to let them know each other for runtimr filter
            newBroadcastJoinOpt.setSkewJoinFriend(newShuffleJoinOpt);
            newShuffleJoinOpt.setSkewJoinFriend(newBroadcastJoinOpt);

            LocalExchangerType localExchangerType =
                    parentRequireEmpty ? LocalExchangerType.DIRECT : LocalExchangerType.PASS_THROUGH;
            PhysicalMergeOperator mergeOperator =
                    buildMergeOperator(opt.getOutputColumns().getColumnRefOperators(columnRefFactory), 2,
                            localExchangerType, originalShuffleJoinOperator.getLimit());


            OptExpression leftSplitConsumerOptExpForShuffleJoin = OptExpression.builder()
                    .setOp(leftSplitConsumerOptForShuffleJoin)
                    .setInputs(Collections.emptyList())
                    .setLogicalProperty(leftSplitProduceOptExp.getLogicalProperty())
                    .setStatistics(leftSplitProduceOptExp.getStatistics())
                    .setCost(leftSplitProduceOptExp.getCost()).build();

            OptExpression rightSplitConsumerOptExpForShuffleJoin = OptExpression.builder()
                    .setOp(rightSplitConsumerOptForShuffleJoin)
                    .setInputs(Collections.emptyList())
                    .setLogicalProperty(rightSplitProduceOptExp.getLogicalProperty())
                    .setStatistics(rightSplitProduceOptExp.getStatistics())
                    .setCost(rightSplitProduceOptExp.getCost()).build();

            OptExpression newShuffleJoin = OptExpression.builder()
                    .setOp(newShuffleJoinOpt)
                    .setInputs(List.of(leftSplitConsumerOptExpForShuffleJoin, rightSplitConsumerOptExpForShuffleJoin))
                    .setLogicalProperty(opt.getLogicalProperty())
                    .setStatistics(opt.getStatistics())
                    .setRequiredProperties(opt.getRequiredProperties())
                    .setCost(opt.getCost()).build();

            OptExpression leftSplitConsumerOptExpForBroadcastJoin = OptExpression.builder()
                    .setOp(leftSplitConsumerOptForBroadcastJoin)
                    .setInputs(Collections.emptyList())
                    .setLogicalProperty(leftSplitProduceOptExp.getLogicalProperty())
                    .setStatistics(leftSplitProduceOptExp.getStatistics())
                    .setCost(leftSplitProduceOptExp.getCost()).build();

            OptExpression rightSplitConsumerOptExpForBroadcastJoin =
                    OptExpression.builder().setOp(rightSplitConsumerOptForBroadcastJoin)
                            .setInputs(Collections.emptyList())
                            .setLogicalProperty(rightSplitProduceOptExp.getLogicalProperty())
                            .setStatistics(rightSplitProduceOptExp.getStatistics())
                            .setCost(rightSplitProduceOptExp.getCost()).build();

            PhysicalPropertySet rightBroadcastProperty =
                    new PhysicalPropertySet(
                            DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()));
            List<PhysicalPropertySet> requiredPropertiesForBroadcastJoin =
                    Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty);
            OptExpression newBroadcastJoin = OptExpression.builder().setOp(newBroadcastJoinOpt)
                    .setInputs(
                            List.of(leftSplitConsumerOptExpForBroadcastJoin, rightSplitConsumerOptExpForBroadcastJoin))
                    .setLogicalProperty(opt.getLogicalProperty())
                    .setStatistics(opt.getStatistics())
                    .setRequiredProperties(requiredPropertiesForBroadcastJoin)
                    .setCost(opt.getCost()).build();

            OptExpression rightChildOfMerge = newBroadcastJoin;
            if (!parentRequireEmpty) {
                // we need add exchange node to make broadcast join's output distribution can be same as shuffle join's
                OptExpression exchangeOptExpForBroadcastJoin =
                        OptExpression.builder().setOp(rightExchangeOp)
                                .setInputs(Collections.singletonList(newBroadcastJoin))
                                .setLogicalProperty(newBroadcastJoin.getLogicalProperty())
                                .setStatistics(newBroadcastJoin.getStatistics())
                                .setCost(newBroadcastJoin.getCost()).build();
                rightChildOfMerge = exchangeOptExpForBroadcastJoin;
            }

            OptExpression mergeOptExp =
                    OptExpression.builder().setOp(mergeOperator).setInputs(List.of(newShuffleJoin, rightChildOfMerge))
                            .setLogicalProperty(opt.getLogicalProperty())
                            .setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp1 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                            .setInputs(List.of(rightSplitProduceOptExp, mergeOptExp))
                            .setLogicalProperty(opt.getLogicalProperty())
                            .setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp2 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                    .setInputs(List.of(leftSplitProduceOptExp, cteAnchorOptExp1))
                    .setLogicalProperty(opt.getLogicalProperty())
                    .setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();

            // if hit once, we give up rewriting the following subtree
            return cteAnchorOptExp2;
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression opt, Boolean parentRequireEmpty) {
            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) opt.getOp();
            boolean requireEmptyForChild = aggOperator.getType().isLocal();
            return visitChild(opt, requireEmptyForChild);
        }

        private boolean isShuffleJoin(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();
            if (!isExchangeWithDistributionType(opt.getInputs().get(0).getOp(),
                    DistributionSpec.DistributionType.SHUFFLE) ||
                    !isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                            DistributionSpec.DistributionType.SHUFFLE)) {
                return false;
            }
            return true;
        }

        private boolean isBroadCastJoin(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();

            return isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                    DistributionSpec.DistributionType.BROADCAST);
        }

        private boolean isSkew(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();
            // respect join hint
            if (joinOperator.getJoinHint().equals(JoinOperator.HINT_SKEW)) {
                return true;
            }
            // right now only support join hint
            return false;
        }

        private boolean isExchangeWithDistributionType(Operator child, DistributionSpec.DistributionType expectedType) {
            if (!(child.getOpType() == OperatorType.PHYSICAL_DISTRIBUTION)) {
                return false;
            }
            PhysicalDistributionOperator operator = (PhysicalDistributionOperator) child;
            return Objects.equals(operator.getDistributionSpec().getType(), expectedType);
        }

        private PhysicalMergeOperator buildMergeOperator(List<ColumnRefOperator> outputColumns, int childNum,
                                                         LocalExchangerType localExchangeType, long limit) {
            List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
            for (int i = 0; i < childNum; i++) {
                childOutputColumns.add(outputColumns);
            }
            return new PhysicalMergeOperator(outputColumns, childOutputColumns, localExchangeType, limit);
        }

        private Pair<ColumnRefOperator, ColumnRefOperator> findSkewColumns(OptExpression input) {
            PhysicalHashJoinOperator oldJoinOperator = (PhysicalHashJoinOperator) input.getOp();
            ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();

            ScalarOperator skewColumn = oldJoinOperator.getSkewColumn();
            ScalarOperator rightSkewColumn = null;

            List<BinaryPredicateOperator> equalConjs = JoinHelper.
                    getEqualsPredicate(leftOutputColumns, rightOutputColumns,
                            Utils.extractConjuncts(oldJoinOperator.getOnPredicate()));
            for (BinaryPredicateOperator equalConj : equalConjs) {
                ScalarOperator child0 = equalConj.getChild(0);
                ScalarOperator child1 = equalConj.getChild(1);
                // skew column may be left or right column of the equal predicate
                if (skewColumn.equals(child0)) {
                    rightSkewColumn = child1;
                    break;
                } else if (skewColumn.equals(child1)) {
                    rightSkewColumn = child0;
                    break;
                } else {
                    // find the skew column in the grandchild(exchange's child) project map
                    if (input.inputAt(0).inputAt(0).getOp().getProjection() != null) {
                        Map<ColumnRefOperator, ScalarOperator> projectMap =
                                input.inputAt(0).inputAt(0).getOp().getProjection().getColumnRefMap();
                        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap);
                        ScalarOperator rewriteChild0 = rewriter.rewrite(child0);
                        ScalarOperator rewriteChild1 = rewriter.rewrite(child1);
                        if ((skewColumn.equals(rewriteChild0)) ||
                                rewriteChild0.isCast() && skewColumn.equals(rewriteChild0.getChild(0)) ||
                                (skewColumn.equals(rewriteChild1)) ||
                                (rewriteChild1.isCast() && skewColumn.equals(rewriteChild1.getChild(0)))) {
                            skewColumn = child0;
                            rightSkewColumn = child1;
                            break;
                        }
                    }
                }
            }
            return Pair.create((ColumnRefOperator) skewColumn, (ColumnRefOperator) rightSkewColumn);
        }

        private Map<ColumnRefOperator, ColumnRefOperator> generateOutputColumnRefMap(
                List<ColumnRefOperator> outputColumns) {
            Map<ColumnRefOperator, ColumnRefOperator> result = new HashMap<>();
            for (ColumnRefOperator columnRefOperator : outputColumns) {
                result.put(columnRefOperator, columnRefOperator);
            }
            return result;
        }
    }

}
