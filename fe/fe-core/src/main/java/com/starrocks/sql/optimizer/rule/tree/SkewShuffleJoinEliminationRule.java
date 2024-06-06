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

import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.LocalExchangerType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class SkewShuffleJoinEliminationRule implements TreeRewriteRule {
    private static AtomicInteger uniqueSplitId;

    private SkewShuffleJoinEliminationVisitor handler = null;

    public SkewShuffleJoinEliminationRule() {
        this.uniqueSplitId = new AtomicInteger();
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        handler = new SkewShuffleJoinEliminationVisitor(taskContext.getOptimizerContext().getColumnRefFactory());
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

            boolean requireEmptyForChild = isBroadCastJoin(opt);
            // doesn't support cross join and right join
            if (originalShuffleJoinOperator.getJoinType().isCrossJoin() ||
                    originalShuffleJoinOperator.getJoinType().isRightJoin()) {
                return visitChild(opt, requireEmptyForChild);
            }

            if (!isShuffleJoin(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            if (!isSkew(opt)) {
                return visitChild(opt, requireEmptyForChild);
            }

            OptExpression leftExchangeOptExp = opt.inputAt(0);
            PhysicalDistributionOperator leftExchangeOp = (PhysicalDistributionOperator) leftExchangeOptExp.getOp();
            OptExpression leftChildOfExchangeOptExp = opt.inputAt(0).inputAt(0);

            OptExpression rightExchangeOptExp = opt.inputAt(1);
            PhysicalDistributionOperator rightExchangeOp = (PhysicalDistributionOperator) rightExchangeOptExp.getOp();
            OptExpression rightChildOfExchangeOptExp = opt.inputAt(1).inputAt(0);

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

            List<ScalarOperator> inPredicateParams = new ArrayList<>();
            inPredicateParams.add(originalShuffleJoinOperator.getSkewColumn());
            inPredicateParams.addAll(originalShuffleJoinOperator.getSkewValues());

            ScalarOperator inSkewPredicate = new InPredicateOperator(false, inPredicateParams);
            ScalarOperator notInSkewPredicate = new InPredicateOperator(true, inPredicateParams);

            PhysicalSplitConsumeOperator leftSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(), notInSkewPredicate,
                            leftExchangeOp.getDistributionSpec());

            PhysicalSplitConsumeOperator leftSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(), inSkewPredicate,
                            leftExchangeOp.getDistributionSpec());

            PhysicalSplitConsumeOperator rightSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(), notInSkewPredicate,
                            rightExchangeOp.getDistributionSpec());

            PhysicalSplitConsumeOperator rightSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(), inSkewPredicate,
                            rightExchangeOp.getDistributionSpec());

            PhysicalHashJoinOperator newShuffleJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), originalShuffleJoinOperator.getProjection(),
                    originalShuffleJoinOperator.getSkewColumn(), originalShuffleJoinOperator.getSkewValues());

            PhysicalHashJoinOperator newBroadcastJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), originalShuffleJoinOperator.getProjection(),
                    originalShuffleJoinOperator.getSkewColumn(), originalShuffleJoinOperator.getSkewValues());

            LocalExchangerType localExchangerType =
                    parentRequireEmpty ? LocalExchangerType.DIRECT : LocalExchangerType.PASS_THROUGH;
            PhysicalMergeOperator mergeOperator =
                    buildMergeOperator(opt.getOutputColumns().getColumnRefOperators(columnRefFactory), 2,
                            localExchangerType, originalShuffleJoinOperator.getLimit());

            // we need add exchange node to make broadcast join's output can be same as shuffle join's output
            //            if (!parentRequireEmpty) {
            //
            //                PhysicalDistributionOperator broadcastExchangeToMergeOp = new PhysicalDistributionOperator(DistributionSpec.createHashDistributionSpec(
            //                        new HashDistributionDesc(List.of(), HashDistributionDesc.SourceType.SHUFFLE_ENFORCE));
            //            }

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

            OptExpression newBroadcastJoin = OptExpression.builder().setOp(newBroadcastJoinOpt)
                    .setInputs(
                            List.of(leftSplitConsumerOptExpForBroadcastJoin, rightSplitConsumerOptExpForBroadcastJoin))
                    .setLogicalProperty(opt.getLogicalProperty())
                    .setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();

            OptExpression mergeOptExp =
                    OptExpression.builder().setOp(mergeOperator).setInputs(List.of(newShuffleJoin, newBroadcastJoin))
                            .setLogicalProperty(opt.getLogicalProperty())
                            .setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp1 =
                    OptExpression.builder().setOp(opt.getOp()).setInputs(List.of(rightSplitProduceOptExp, mergeOptExp))
                            .setLogicalProperty(opt.getLogicalProperty())
                            .setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp2 = OptExpression.builder().setOp(opt.getOp())
                    .setInputs(List.of(leftSplitProduceOptExp, cteAnchorOptExp1))
                    .setLogicalProperty(opt.getLogicalProperty())
                    .setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();

            return cteAnchorOptExp2;

            // temp
            //            return visitChild(opt, requireEmptyForChild);
        }

        @Override
        public OptExpression visitPhysicalHashAggregate(OptExpression opt, Boolean parentRequireEmpty) {
            PhysicalHashAggregateOperator aggOperator = (PhysicalHashAggregateOperator) opt.getOp();
            boolean requireEmptyForChild = aggOperator.getType().isLocal();
            return visitChild(opt, requireEmptyForChild);
        }

        private boolean isShuffleJoin(OptExpression opt) {
            PhysicalJoinOperator joinOperator = (PhysicalJoinOperator) opt.getOp();
            // right now only support shuffle join
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
    }

}
