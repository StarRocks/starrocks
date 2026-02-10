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
<<<<<<< HEAD
import com.starrocks.analysis.JoinOperator;
=======
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalConcatenateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/* rewrite the tree top down
 * if one shuffle join op is decided as skew, rewrite it as below and do not visit its children
 *        shuffle join                       concatenate
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
    private final AtomicInteger uniqueSplitId;

    private SkewShuffleJoinEliminationVisitor handler = null;

    public SkewShuffleJoinEliminationRule() {
        this.uniqueSplitId = new AtomicInteger();
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        handler = new SkewShuffleJoinEliminationVisitor(taskContext.getOptimizerContext().getColumnRefFactory());
        return root.getOp().accept(handler, root, null);
    }

    class SkewColumnAndValues {
        List<ScalarOperator> skewValues;
        Pair<ScalarOperator, ScalarOperator> skewColumns;

        public SkewColumnAndValues(List<ScalarOperator> skewValues, Pair<ScalarOperator, ScalarOperator> skewColumns) {
            this.skewValues = skewValues;
            this.skewColumns = skewColumns;
        }
    }

    class SplitProducerAndConsumer {
        OptExpression splitProducer;
        OptExpression splitConsumerOptForShuffleJoin;
        OptExpression splitConsumerOptForBroadcastJoin;

        public SplitProducerAndConsumer(OptExpression splitProducer, OptExpression splitConsumerOptForShuffleJoin,
                                        OptExpression splitConsumerOptForBroadcastJoin) {
            this.splitProducer = splitProducer;
            this.splitConsumerOptForShuffleJoin = splitConsumerOptForShuffleJoin;
            this.splitConsumerOptForBroadcastJoin = splitConsumerOptForBroadcastJoin;
        }
    }

    private class SkewShuffleJoinEliminationVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory columnRefFactory;

        public SkewShuffleJoinEliminationVisitor(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        private record OutputExchangeTemplate(DistributionSpec distributionSpec, ColumnRefSet usedColumns) {
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Void context) {
            return visitChild(optExpr, context);
        }

        private OptExpression visitChild(OptExpression opt, Void context) {
            for (int idx = 0; idx < opt.arity(); ++idx) {
                OptExpression child = opt.inputAt(idx);
                opt.setChild(idx, child.getOp().accept(this, child, context));
            }
            return opt;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression opt, Void context) {
            PhysicalHashJoinOperator originalShuffleJoinOperator = opt.getOp().cast();

            if (!canOptimize(originalShuffleJoinOperator, opt)) {
                return visitChild(opt, context);
            }

            // find skew columns, rewrite skew values if needed
            SkewColumnAndValues skewColumnAndValues = findSkewColumns(opt);
            ScalarOperator leftSkewColumn = skewColumnAndValues.skewColumns.first;
            ScalarOperator rightSkewColumn = skewColumnAndValues.skewColumns.second;
            List<ScalarOperator> skewValues = skewColumnAndValues.skewValues;
            if (leftSkewColumn == null || rightSkewColumn == null || skewValues == null || skewValues.isEmpty()) {
                return opt;
            }

            // rewrite plan
<<<<<<< HEAD
            SplitProducerAndConsumer leftSplitProducerAndConsumer =
                    generateSplitProducerAndConsumer(opt.inputAt(0), leftSkewColumn, skewValues, true);

            SplitProducerAndConsumer rightSplitProducerAndConsumer =
                    generateSplitProducerAndConsumer(opt.inputAt(1), rightSkewColumn, skewValues, false);
=======
            OptExpression leftChild = opt.inputAt(0);
            OptExpression rightChild = opt.inputAt(1);

            // Map join key exprs from (skew side / non-skew side) into (left input / right input).
            // This keeps the shuffle-join input order unchanged.
            ScalarOperator leftInputJoinKeyExpr =
                    (skewSideChildIndex == 0) ? skewSideJoinKeyExpr : nonSkewSideJoinKeyExpr;
            ScalarOperator rightInputJoinKeyExpr =
                    (skewSideChildIndex == 0) ? nonSkewSideJoinKeyExpr : skewSideJoinKeyExpr;

            boolean isLeftOuterJoin = originalShuffleJoinOperator.getJoinType().isAnyLeftOuterJoin();
            boolean skewOnLeft = skewSideChildIndex == 0;
            // For LEFT OUTER JOIN, the left input is the preserved side. Only preserved-side NULL skew is safe to optimize.
            boolean leftOuterJoinWithPreservedSideNullSkew = isLeftOuterJoin && skewOnLeft && includeNullSkew;
            boolean nullOnlyLeftOuterJoinFastPath =
                    leftOuterJoinWithPreservedSideNullSkew && nonNullSkewValues.isEmpty();

            SplitProducerAndConsumer leftSplit =
                    generateSplitProducerAndConsumer(leftChild, leftInputJoinKeyExpr, nonNullSkewValues,
                            includeNullSkew, skewOnLeft);

            SplitProducerAndConsumer rightSplit;
            if (nullOnlyLeftOuterJoinFastPath) {
                // NULL-only skew: right side does not participate in the skew branch at all.
                rightSplit = buildSplit(rightChild, ConstantOperator.FALSE, ConstantOperator.TRUE, false);
            } else {
                rightSplit = generateSplitProducerAndConsumer(rightChild, rightInputJoinKeyExpr, nonNullSkewValues,
                        includeNullSkew, !skewOnLeft);
            }
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))

            // keep projection for new join opt
            Projection projectionOnJoin = originalShuffleJoinOperator.getProjection();

            PhysicalHashJoinOperator newShuffleJoinOpt =
                    new PhysicalHashJoinOperator(originalShuffleJoinOperator.getJoinType(),
                            originalShuffleJoinOperator.getOnPredicate(), originalShuffleJoinOperator.getJoinHint(),
                            originalShuffleJoinOperator.getLimit(), originalShuffleJoinOperator.getPredicate(),
                            projectionOnJoin, leftSkewColumn, skewValues);

            PhysicalHashJoinOperator newBroadcastJoinOpt =
                    new PhysicalHashJoinOperator(originalShuffleJoinOperator.getJoinType(),
                            originalShuffleJoinOperator.getOnPredicate(), originalShuffleJoinOperator.getJoinHint(),
                            originalShuffleJoinOperator.getLimit(), originalShuffleJoinOperator.getPredicate(),
                            projectionOnJoin, leftSkewColumn, skewValues);

            // we have to let them know each other for runtimr filter
            newBroadcastJoinOpt.setSkewJoinFriend(newShuffleJoinOpt);
            newShuffleJoinOpt.setSkewJoinFriend(newBroadcastJoinOpt);
            newBroadcastJoinOpt.setSkewSideChildIndex(skewSideChildIndex);
            newShuffleJoinOpt.setSkewSideChildIndex(skewSideChildIndex);

            OutputExchangeTemplate outputExchangeTemplate = getOutputExchangeTemplateIfRequired(opt, skewSideChildIndex);
            final boolean requireOutputDistribution = outputExchangeTemplate != null;
            LocalExchangerType localExchangerType =
<<<<<<< HEAD
                    opt.isExistRequiredDistribution() ? LocalExchangerType.PASS_THROUGH : LocalExchangerType.DIRECT;
            PhysicalConcatenateOperator concatenateOperator =
                    buildConcatenateOperator(opt.getOutputColumns().getColumnRefOperators(columnRefFactory), 2,
                            localExchangerType, originalShuffleJoinOperator.getLimit());

            OptExpression newShuffleJoin = OptExpression.builder().setOp(newShuffleJoinOpt).setInputs(
                            List.of(leftSplitProducerAndConsumer.splitConsumerOptForShuffleJoin,
                                    rightSplitProducerAndConsumer.splitConsumerOptForShuffleJoin))
=======
                    requireOutputDistribution ? LocalExchangerType.PASS_THROUGH : LocalExchangerType.DIRECT;
            List<ColumnRefOperator> outputColumns =
                    opt.getOutputColumns().getColumnRefOperators(columnRefFactory);
            if (originalShuffleJoinOperator.getJoinType().isAnyLeftOuterJoin()) {
                ColumnRefSet rightOutputColumns = opt.inputAt(1).getOutputColumns();
                for (ColumnRefOperator outputColumn : outputColumns) {
                    if (rightOutputColumns.contains(outputColumn)) {
                        outputColumn.setNullable(true);
                    }
                }
            }
            PhysicalConcatenateOperator concatenateOperator = buildConcatenateOperator(outputColumns,
                    localExchangerType, originalShuffleJoinOperator.getLimit());

            OptExpression newShuffleJoin = OptExpression.builder().setOp(newShuffleJoinOpt).setInputs(
                            newArrayList(leftSplit.splitConsumerOptForShuffleJoin,
                                    rightSplit.splitConsumerOptForShuffleJoin))
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setRequiredProperties(opt.getRequiredProperties()).setCost(opt.getCost()).build();

            PhysicalPropertySet rightBroadcastProperty = new PhysicalPropertySet(
                    DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()));
            List<PhysicalPropertySet> requiredPropertiesForBroadcastJoin =
                    Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty);
<<<<<<< HEAD
            OptExpression newBroadcastJoin = OptExpression.builder().setOp(newBroadcastJoinOpt).setInputs(
                            List.of(leftSplitProducerAndConsumer.splitConsumerOptForBroadcastJoin,
                                    rightSplitProducerAndConsumer.splitConsumerOptForBroadcastJoin))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setRequiredProperties(requiredPropertiesForBroadcastJoin).setCost(opt.getCost()).build();

            OptExpression rightChildOfConcatenate = newBroadcastJoin;
            if (opt.isExistRequiredDistribution()) {
                OptExpression rightExchangeOptExp = opt.inputAt(1);
                PhysicalDistributionOperator rightExchangeOpOfOriginalShuffleJoin =
                        (PhysicalDistributionOperator) rightExchangeOptExp.getOp();
=======

            ColumnRefSet usedColumnsOfOutputExchangeOp =
                    outputExchangeTemplate == null ? null : outputExchangeTemplate.usedColumns();
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))

            OptExpression skewBranch = nullOnlyLeftOuterJoinFastPath
                    ? buildNullFastBranch(opt, leftSplit.splitConsumerOptForBroadcastJoin, leftChild, rightChild,
                    outputColumns, usedColumnsOfOutputExchangeOp)
                    : buildBroadcastJoinBranch(opt, requiredPropertiesForBroadcastJoin, newBroadcastJoinOpt,
                            skewSideChildIndex, leftSplit, rightSplit);

            OptExpression rightChildOfConcatenate = skewBranch;
            if (requireOutputDistribution) {
                PhysicalDistributionOperator newExchangeForBroadcastJoin =
                        new PhysicalDistributionOperator(outputExchangeTemplate.distributionSpec());
                // we need add exchange node to make broadcast join's output distribution can be same as shuffle join's
<<<<<<< HEAD
                OptExpression exchangeOptExpForBroadcastJoin =
                        OptExpression.builder().setOp(newExchangeForBroadcastJoin)
                                .setInputs(Collections.singletonList(newBroadcastJoin))
                                .setLogicalProperty(newBroadcastJoin.getLogicalProperty())
                                .setStatistics(newBroadcastJoin.getStatistics()).setCost(newBroadcastJoin.getCost())
                                .build();
                rightChildOfConcatenate = exchangeOptExpForBroadcastJoin;
                if (projectionOnJoin != null) {
                    // broadcast join's projection should keep the columns used in exchange
                    Projection projectionForBroadcast = projectionOnJoin.deepClone();

                    ColumnRefSet usedColumnsOfRightExchangeOp =
                            rightExchangeOpOfOriginalShuffleJoin.getRowOutputInfo(rightExchangeOptExp.getInputs())
                                    .getUsedColumnRefSet();
                    usedColumnsOfRightExchangeOp.except(projectionOnJoin.getOutputColumns());
                    usedColumnsOfRightExchangeOp.getColumnRefOperators(columnRefFactory).stream()
                            .forEach(columnRefOperator -> {
                                projectionForBroadcast.getColumnRefMap()
                                        .putIfAbsent(columnRefOperator, columnRefOperator);
                            });
=======
                rightChildOfConcatenate = OptExpression.builder().setOp(newExchangeForBroadcastJoin)
                        .setInputs(Collections.singletonList(skewBranch))
                        .setLogicalProperty(skewBranch.getLogicalProperty())
                        .setStatistics(skewBranch.getStatistics()).setCost(skewBranch.getCost())
                        .build();

                if (!nullOnlyLeftOuterJoinFastPath && projectionOnJoin != null) {
                    // broadcast join's projection should keep the columns used in exchange
                    Projection projectionForBroadcast = projectionOnJoin.deepClone();
                    addIdentityColumnsToProjectionIfMissing(projectionForBroadcast,
                            projectionOnJoin.getOutputColumns(),
                            usedColumnsOfOutputExchangeOp);
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))
                    newBroadcastJoinOpt.setProjection(projectionForBroadcast);
                }
            }

            OptExpression concatenateOptExp = OptExpression.builder().setOp(concatenateOperator)
                    .setInputs(List.of(newShuffleJoin, rightChildOfConcatenate))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp1 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
<<<<<<< HEAD
                            .setInputs(List.of(rightSplitProducerAndConsumer.splitProducer, concatenateOptExp))
                            .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp2 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                            .setInputs(List.of(leftSplitProducerAndConsumer.splitProducer, cteAnchorOptExp1))
=======
                            .setInputs(newArrayList(rightSplit.splitProducer, concatenateOptExp))
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))
                            .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            // if hit once, we give up rewriting the following subtree
<<<<<<< HEAD
            return cteAnchorOptExp2;
=======
            return OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                    .setInputs(newArrayList(leftSplit.splitProducer, cteAnchorOptExp1))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))
        }

        private SplitProducerAndConsumer generateSplitProducerAndConsumer(OptExpression exchangeOptExp,
                                                                          ScalarOperator skewColumn,
                                                                          List<ScalarOperator> skewValues,
<<<<<<< HEAD
                                                                          boolean isLeft) {
            PhysicalDistributionOperator exchangeOpOfOriginalShuffleJoin =
                    (PhysicalDistributionOperator) exchangeOptExp.getOp();

            PhysicalSplitProduceOperator splitProduceOperator =
                    new PhysicalSplitProduceOperator(uniqueSplitId.getAndIncrement());
            OptExpression splitProduceOptExp =
                    OptExpression.builder().setOp(splitProduceOperator).setInputs(exchangeOptExp.getInputs())
                            .setLogicalProperty(exchangeOptExp.getLogicalProperty())
                            .setStatistics(exchangeOptExp.getStatistics()).setCost(exchangeOptExp.getCost()).build();

            Pair<ScalarOperator, ScalarOperator> tablePredicates = generateInAndNotInPredicate(skewColumn, skewValues);
=======
                                                                          boolean includeNullSkew,
                                                                          boolean isLeftInputInBroadcastJoin) {
            Pair<ScalarOperator, ScalarOperator> tablePredicates =
                    generateSkewAndNonSkewPredicate(skewColumn, skewValues, includeNullSkew);
            return buildSplit(exchangeOptExp, tablePredicates.first, tablePredicates.second, isLeftInputInBroadcastJoin);
        }

        private SplitProducerAndConsumer buildSplit(OptExpression exchangeOptExp,
                                                    ScalarOperator skewPredicate,
                                                    ScalarOperator nonSkewPredicate,
                                                    boolean isLeftInputInBroadcastJoin) {
            PhysicalDistributionOperator exchangeOpOfOriginalShuffleJoin = exchangeOptExp.getOp().cast();

            PhysicalSplitProduceOperator splitProduceOperator =
                    new PhysicalSplitProduceOperator(uniqueSplitId.getAndIncrement());
            OptExpression splitProduceOptExp = OptExpression.builder()
                    .setOp(splitProduceOperator)
                    .setInputs(exchangeOptExp.getInputs())
                    .setLogicalProperty(exchangeOptExp.getLogicalProperty())
                    .setStatistics(exchangeOptExp.getStatistics())
                    .setCost(exchangeOptExp.getCost())
                    .build();
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))

            List<ColumnRefOperator> splitOutputColumns =
                    exchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory);

            DistributionSpec distributionSpecForBroadCastJoin =
                    isLeftInputInBroadcastJoin ? new RoundRobinDistributionSpec()
                            : DistributionSpec.createReplicatedDistributionSpec();

            PhysicalSplitConsumeOperator splitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(splitProduceOperator.getSplitId(), nonSkewPredicate,
                            exchangeOpOfOriginalShuffleJoin.getDistributionSpec(), splitOutputColumns);
            PhysicalSplitConsumeOperator splitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(splitProduceOperator.getSplitId(), skewPredicate,
                            distributionSpecForBroadCastJoin, splitOutputColumns);

            OptExpression splitConsumerOptExpForShuffleJoin =
                    OptExpression.builder().setOp(splitConsumerOptForShuffleJoin).setInputs(Collections.emptyList())
                            .setLogicalProperty(splitProduceOptExp.getLogicalProperty())
                            .setStatistics(splitProduceOptExp.getStatistics()).setCost(splitProduceOptExp.getCost())
                            .build();

            OptExpression splitConsumerOptExpForBroadcastJoin =
                    OptExpression.builder().setOp(splitConsumerOptForBroadcastJoin).setInputs(Collections.emptyList())
                            .setLogicalProperty(splitProduceOptExp.getLogicalProperty())
                            .setStatistics(splitProduceOptExp.getStatistics()).setCost(splitProduceOptExp.getCost())
                            .build();

            return new SplitProducerAndConsumer(splitProduceOptExp, splitConsumerOptExpForShuffleJoin,
                    splitConsumerOptExpForBroadcastJoin);
        }

<<<<<<< HEAD
        private Pair<ScalarOperator, ScalarOperator> generateInAndNotInPredicate(ScalarOperator skewColumn,
                                                                                 List<ScalarOperator> skewValues) {
            List<ScalarOperator> inPredicateParams = new ArrayList<>();
            inPredicateParams.add(skewColumn);
            inPredicateParams.addAll(skewValues);
=======
        private OptExpression buildBroadcastJoinBranch(OptExpression opt,
                                                      List<PhysicalPropertySet> requiredPropertiesForBroadcastJoin,
                                                      PhysicalHashJoinOperator newBroadcastJoinOpt,
                                                      int skewSideChildIndex,
                                                      SplitProducerAndConsumer leftSplit,
                                                      SplitProducerAndConsumer rightSplit) {
            boolean skewOnRight = skewSideChildIndex == 1;
            // If skew is on the original right child, swap inputs ONLY for the broadcast branch so that
            // skew side becomes the left(probe/round-robin) input and the other side is broadcast(build).
            OptExpression broadcastLeft = skewOnRight ? rightSplit.splitConsumerOptForBroadcastJoin
                    : leftSplit.splitConsumerOptForBroadcastJoin;
            OptExpression broadcastRight = skewOnRight ? leftSplit.splitConsumerOptForBroadcastJoin
                    : rightSplit.splitConsumerOptForBroadcastJoin;

            return OptExpression.builder().setOp(newBroadcastJoinOpt).setInputs(newArrayList(broadcastLeft, broadcastRight))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setRequiredProperties(requiredPropertiesForBroadcastJoin).setCost(opt.getCost()).build();
        }

        private OptExpression buildNullFastBranch(OptExpression joinOpt,
                                                 OptExpression leftNullRows,
                                                 OptExpression leftChild,
                                                 OptExpression rightChild,
                                                 List<ColumnRefOperator> outputColumns,
                                                 ColumnRefSet requiredColumnsForOutputExchange) {
            ColumnRefSet leftOutputCols = leftChild.getOutputColumns();
            ColumnRefSet rightOutputCols = rightChild.getOutputColumns();
            Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
            for (ColumnRefOperator outputColumn : outputColumns) {
                if (leftOutputCols.contains(outputColumn)) {
                    columnRefMap.put(outputColumn, outputColumn);
                } else if (rightOutputCols.contains(outputColumn)) {
                    columnRefMap.put(outputColumn, ConstantOperator.createNull(outputColumn.getType()));
                } else {
                    Map<ColumnRefOperator, ScalarOperator> joinOutput = joinOpt.getRowOutputInfo().getColumnRefMap();
                    columnRefMap.put(outputColumn, joinOutput.getOrDefault(outputColumn, outputColumn));
                }
            }

            addTypedNullColumnsIfMissing(columnRefMap, requiredColumnsForOutputExchange);
            PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(columnRefMap, Collections.emptyMap());
            return OptExpression.builder().setOp(projectOperator)
                    .setInputs(Collections.singletonList(leftNullRows))
                    .setLogicalProperty(joinOpt.getLogicalProperty())
                    .setStatistics(joinOpt.getStatistics()).setCost(joinOpt.getCost())
                    .build();
        }


        private OutputExchangeTemplate getOutputExchangeTemplateIfRequired(OptExpression joinOpt, int skewSideChildIndex) {
            if (!joinOpt.isExistRequiredDistribution()) {
                return null;
            }
            OptExpression template = joinOpt.inputAt(1 - skewSideChildIndex);
            PhysicalDistributionOperator exchangeOp = template.getOp().cast();
            ColumnRefSet used = exchangeOp.getRowOutputInfo(template.getInputs()).getUsedColumnRefSet();
            return new OutputExchangeTemplate(exchangeOp.getDistributionSpec(), used);
        }

        private void addIdentityColumnsToProjectionIfMissing(Projection projection,
                                                             List<ColumnRefOperator> alreadyOutputColumns,
                                                             ColumnRefSet requiredColumns) {
            if (projection == null || requiredColumns == null || requiredColumns.isEmpty()) {
                return;
            }
            ColumnRefSet alreadyOutputSet = new ColumnRefSet(alreadyOutputColumns);
            ColumnRefSet missing = requiredColumns.clone();
            missing.except(alreadyOutputSet);
            for (ColumnRefOperator c : missing.getColumnRefOperators(columnRefFactory)) {
                projection.getColumnRefMap().putIfAbsent(c, c);
            }
        }

        private void addTypedNullColumnsIfMissing(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                  ColumnRefSet requiredColumns) {
            if (requiredColumns == null || requiredColumns.isEmpty()) {
                return;
            }
            for (ColumnRefOperator c : requiredColumns.getColumnRefOperators(columnRefFactory)) {
                columnRefMap.putIfAbsent(c, ConstantOperator.createNull(c.getType()));
            }
        }

        /**
         * Build split predicates for skew/non-skew branches.
         * <p>
         * - Skew branch (broadcast join): {@code IN (nonNullSkewValues)} and/or {@code IS NULL} (if includeNullSkew)
         * - Non-skew branch (shuffle join): the complement of skew branch
         * <p>
         * NOTE: Do not put NULL into IN-list due to SQL three-valued logic. Use explicit IS NULL/IS NOT NULL instead.
         * <p>
         * Examples (let join key be {@code k}):
         * <ul>
         *   <li>Only non-NULL skew values (S={1,2,3}, includeNullSkew=false):
         *     <ul>
         *       <li>Skew branch: {@code k IN (1,2,3)}</li>
         *       <li>Non-skew branch: {@code k NOT IN (1,2,3) OR k IS NULL}</li>
         *     </ul>
         *   </li>
         *   <li>Only NULL skew (S={}, includeNullSkew=true):
         *     <ul>
         *       <li>Skew branch: {@code k IS NULL}</li>
         *       <li>Non-skew branch: {@code k IS NOT NULL}</li>
         *     </ul>
         *   </li>
         *   <li>Both non-NULL and NULL skew (S={1,2,3}, includeNullSkew=true):
         *     <ul>
         *       <li>Skew branch: {@code k IN (1,2,3) OR k IS NULL}</li>
         *       <li>Non-skew branch: {@code k NOT IN (1,2,3) AND k IS NOT NULL}</li>
         *     </ul>
         *   </li>
         * </ul>
         */
        private Pair<ScalarOperator, ScalarOperator> generateSkewAndNonSkewPredicate(ScalarOperator skewColumn,
                                                                                     List<ScalarOperator> skewValues,
                                                                                     boolean includeNullSkew) {
            ScalarOperator inSkewPredicate = null;
            if (skewValues != null && !skewValues.isEmpty()) {
                List<ScalarOperator> inPredicateParams = new ArrayList<>();
                inPredicateParams.add(skewColumn);
                inPredicateParams.addAll(skewValues);
                inSkewPredicate = new InPredicateOperator(false, inPredicateParams);
                ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
                inSkewPredicate = rewriter.rewrite(inSkewPredicate,
                        ImmutableList.of(new ImplicitCastRule(), new ReduceCastRule(), new FoldConstantsRule()));
            }
>>>>>>> df3ac94afc ([Enhancement] Improve skew join v2 rewrite (#68886))

            ScalarOperator inSkewPredicate = new InPredicateOperator(false, inPredicateParams);
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            inSkewPredicate =
                    rewriter.rewrite(inSkewPredicate, ImmutableList.of(new ImplicitCastRule(), new ReduceCastRule()));
            ScalarOperator notInSkewPredicate = new InPredicateOperator(true, inSkewPredicate.getChildren());
            ScalarOperator notInSkewOrNullPredicate =
                    CompoundPredicateOperator.or(notInSkewPredicate, new IsNullPredicateOperator(skewColumn));

            return Pair.create(inSkewPredicate, notInSkewOrNullPredicate);

        }

        private boolean isExchangeWithDistributionType(Operator child, DistributionSpec.DistributionType expectedType) {
            if (!(child.getOpType() == OperatorType.PHYSICAL_DISTRIBUTION)) {
                return false;
            }
            PhysicalDistributionOperator operator = (PhysicalDistributionOperator) child;
            return Objects.equals(operator.getDistributionSpec().getType(), expectedType);
        }

        private PhysicalConcatenateOperator buildConcatenateOperator(List<ColumnRefOperator> outputColumns,
                                                                     int childNum, LocalExchangerType localExchangeType,
                                                                     long limit) {
            List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
            for (int i = 0; i < childNum; i++) {
                childOutputColumns.add(outputColumns);
            }
            return new PhysicalConcatenateOperator(outputColumns, childOutputColumns, localExchangeType, limit);
        }

        private SkewColumnAndValues findSkewColumns(OptExpression input) {
            PhysicalHashJoinOperator oldJoinOperator = (PhysicalHashJoinOperator) input.getOp();
            ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();

            // right now skew hint only support skew in left table
            // originalSkewColumn is column in left table which is skew, and skew values have same type with originalSkewColumn
            ScalarOperator originalSkewColumn = oldJoinOperator.getSkewColumn();
            ScalarOperator leftSkewColumn = null;
            ScalarOperator rightSkewColumn = null;
            List<ScalarOperator> skewValues = oldJoinOperator.getSkewValues();

            // get equal conjuncts from on predicate, every eq predicate's child can't be constant
            List<BinaryPredicateOperator> equalConjs =
                    JoinHelper.getEqualsPredicate(leftOutputColumns, rightOutputColumns,
                            Utils.extractConjuncts(oldJoinOperator.getOnPredicate()));
            for (BinaryPredicateOperator equalConj : equalConjs) {
                ScalarOperator child0 = equalConj.getChild(0);
                ScalarOperator child1 = equalConj.getChild(1);

                Projection leftProjection = input.inputAt(0).inputAt(0).getOp().getProjection();

                // firstly,we need to find rightSkewColumn in equalConj. if one side of equalConj using originalSkewColumn
                // then another side of equalConj is rightSkewColumn
                // Besides, we want to know whether originalSkewColumn is equalConj's child or it is part of some expr
                // for example: select xx from t1 join[skew|t1.c_tinyint(1,2,3)] t2 on t1.c_tinyint = t2.int
                // then on predicate will be: cast(t1.c_tinyint as int) = t2.int, in this case we have to rewrite skew values with this expr
                if (child0.equals(originalSkewColumn)) {
                    leftSkewColumn = originalSkewColumn;
                    rightSkewColumn = child1;
                } else if (child1.equals(originalSkewColumn)) {
                    leftSkewColumn = originalSkewColumn;
                    rightSkewColumn = child0;
                } else {
                    // originalSkewColumn is part of some expr
                    if (leftProjection != null) {
                        ScalarOperator rewriteChild0 = replaceColumnRef(child0, leftProjection);
                        ScalarOperator rewriteChild1 = replaceColumnRef(child1, leftProjection);
                        if (rewriteChild0.getUsedColumns().contains((ColumnRefOperator) originalSkewColumn)) {
                            leftSkewColumn = child0;
                            rightSkewColumn = child1;
                        } else if (rewriteChild1.getUsedColumns().contains((ColumnRefOperator) originalSkewColumn)) {
                            rightSkewColumn = child0;
                            leftSkewColumn = child1;
                        }

                        Map<ColumnRefOperator, ScalarOperator> leftColumnRefMap = leftProjection.getAllMaps();
                        if (leftColumnRefMap.containsKey(leftSkewColumn)) {
                            // this means leftSkewColumn is not a simple columnRef operator, so we need to rewrite skew values
                            // for example: if leftSkewColumn is  cast(t1.c_tinyint as int), and skew values are (1,2,3)
                            // then we need to rewrite skew values as (cast(1 as int), cast(2 as int), cast(3 as int))
                            skewValues = rewriteSkewValues(skewValues, leftSkewColumn);
                        }
                    }
                }
            }

            return new SkewColumnAndValues(skewValues, Pair.create(leftSkewColumn, rightSkewColumn));
        }

        private ScalarOperator replaceColumnRef(ScalarOperator scalarOperator, Projection projection) {
            if (projection == null) {
                return scalarOperator;
            }
            Map<ColumnRefOperator, ScalarOperator> projectMap = projection.getAllMaps();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectMap, true);
            return rewriter.rewrite(scalarOperator);
        }

        private List<ScalarOperator> rewriteSkewValues(List<ScalarOperator> originalSkewValues,
                                                       ScalarOperator predicate) {
            List<ScalarOperator> result = new LinkedList<>();
            // for each skew value, we replace the columnRef in left  with skew value
            originalSkewValues.forEach(value -> {
                ColumnRefReplacer refReplacer = new ColumnRefReplacer(value);
                ScalarOperator newSkewValue = predicate.accept(refReplacer, null);
                result.add(newSkewValue);
            });
            return result;
        }

        private boolean canOptimize(PhysicalHashJoinOperator joinOp, OptExpression opt) {
            return isValidJoinType(joinOp) && isShuffleJoin(opt) && isSkew(opt);
        }

        // currently only support inner join and left outer join
        private boolean isValidJoinType(PhysicalHashJoinOperator joinOp) {
            return joinOp.getJoinType().isInnerJoin() || joinOp.getJoinType().isLeftOuterJoin();
        }

        private boolean isShuffleJoin(OptExpression opt) {
            if (opt.getOp().getOpType() != OperatorType.PHYSICAL_HASH_JOIN) {
                return false;
            }
            return isExchangeWithDistributionType(opt.getInputs().get(0).getOp(),
                    DistributionSpec.DistributionType.SHUFFLE) &&
                    isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                            DistributionSpec.DistributionType.SHUFFLE);
        }

        // right now only support join hint with left skew table, since left table is bigger
        // TODO(jerry): support MCV-based skew detection
        private boolean isSkew(OptExpression opt) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) opt.getOp();

            if (joinOperator.getSkewColumn() == null || joinOperator.getSkewValues() == null ||
                    joinOperator.getSkewValues().isEmpty()) {
                return false;
            }

            // respect join hint
            return joinOperator.getJoinHint().equals(JoinOperator.HINT_SKEW);
        }

        private class ColumnRefReplacer extends BaseScalarOperatorShuttle {

            private final ScalarOperator scalarOperator;

            public ColumnRefReplacer(ScalarOperator scalarOperator) {
                this.scalarOperator = scalarOperator;
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                return scalarOperator;
            }

        }
    }

}
