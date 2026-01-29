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
import com.starrocks.analysis.HintNode;
import com.starrocks.common.LocalExchangerType;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.skew.DataSkew;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    private SessionVariable sessionVariable;

    public SkewShuffleJoinEliminationRule() {
        this.uniqueSplitId = new AtomicInteger();
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        SkewShuffleJoinEliminationVisitor handler = new SkewShuffleJoinEliminationVisitor(
                taskContext.getOptimizerContext().getColumnRefFactory());
        return root.getOp().accept(handler, root, null);
    }

    record SkewJoinSplitInfo(List<ScalarOperator> nonNullSkewValues,
                             ScalarOperator skewSideJoinKeyExpr,
                             ScalarOperator nonSkewSideJoinKeyExpr,
                             int skewSideChildIndex,
                             boolean includeNullSkew) {
    }

    record SplitProducerAndConsumer(OptExpression splitProducer,
                                    OptExpression splitConsumerOptForShuffleJoin,
                                    OptExpression splitConsumerOptForBroadcastJoin) {
    }

    private class SkewShuffleJoinEliminationVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory columnRefFactory;

        public SkewShuffleJoinEliminationVisitor(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
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
            PhysicalHashJoinOperator originalShuffleJoinOperator = (PhysicalHashJoinOperator) opt.getOp();

            if (!canOptimize(originalShuffleJoinOperator, opt)) {
                return visitChild(opt, context);
            }

            SkewJoinSplitInfo splitInfo;
            if (isSkew(opt)) {
                // Hint-based skew join (manual).
                ScalarOperator originalSkewColumn = originalShuffleJoinOperator.getSkewColumn();
                if (!(originalSkewColumn instanceof ColumnRefOperator skewColumnRef)) {
                    return visitChild(opt, context);
                }

                List<ScalarOperator> hintSkewValues = originalShuffleJoinOperator.getSkewValues();
                if (hintSkewValues == null || hintSkewValues.isEmpty()) {
                    return visitChild(opt, context);
                }
                boolean includeNullSkew = hintSkewValues.stream().anyMatch(ScalarOperator::isConstantNull);
                List<ScalarOperator> nonNullSkewValues = hintSkewValues.stream()
                        .filter(v -> !v.isConstantNull())
                        .toList();

                splitInfo = findSkewJoinSplitInfo(opt, skewColumnRef, nonNullSkewValues, includeNullSkew);
            } else {
                // Auto-detect skew from statistics (MCV/NULL skew).
                Optional<SkewJoinSplitInfo> detected = detectSkewFromStats(opt);
                if (detected.isEmpty()) {
                    return visitChild(opt, context);
                }
                splitInfo = detected.get();
            }

            ScalarOperator skewSideJoinKeyExpr = splitInfo.skewSideJoinKeyExpr;
            ScalarOperator nonSkewSideJoinKeyExpr = splitInfo.nonSkewSideJoinKeyExpr;
            List<ScalarOperator> nonNullSkewValues = splitInfo.nonNullSkewValues;
            boolean includeNullSkew = splitInfo.includeNullSkew;
            if (skewSideJoinKeyExpr == null || nonSkewSideJoinKeyExpr == null || nonNullSkewValues == null ||
                    (nonNullSkewValues.isEmpty() && !includeNullSkew)) {
                return visitChild(opt, context);
            }
            int skewSideChildIndex = splitInfo.skewSideChildIndex;

            // if this is a left join, and the right side is skew side, we cannot do optimization because
            // the right side's broadcast will cause result incorrect.
            if (originalShuffleJoinOperator.getJoinType().isAnyLeftOuterJoin() && skewSideChildIndex == 1) {
                return visitChild(opt, context);
            }

            // rewrite plan
            OptExpression skewSideChild = opt.inputAt(skewSideChildIndex);
            OptExpression nonSkewSideChild = opt.inputAt(1 - skewSideChildIndex);

            SplitProducerAndConsumer skewSideSplit = generateSplitProducerAndConsumer(skewSideChild, skewSideJoinKeyExpr,
                    nonNullSkewValues, includeNullSkew, true);

            SplitProducerAndConsumer nonSkewSideSplit =
                    generateSplitProducerAndConsumer(nonSkewSideChild, nonSkewSideJoinKeyExpr,
                            nonNullSkewValues, includeNullSkew, false);

            // keep projection for new join opt
            Projection projectionOnJoin = originalShuffleJoinOperator.getProjection();

            PhysicalHashJoinOperator newShuffleJoinOpt =
                    new PhysicalHashJoinOperator(originalShuffleJoinOperator.getJoinType(),
                            originalShuffleJoinOperator.getOnPredicate(), originalShuffleJoinOperator.getJoinHint(),
                            originalShuffleJoinOperator.getLimit(), originalShuffleJoinOperator.getPredicate(),
                            projectionOnJoin, skewSideJoinKeyExpr, nonNullSkewValues);

            PhysicalHashJoinOperator newBroadcastJoinOpt =
                    new PhysicalHashJoinOperator(originalShuffleJoinOperator.getJoinType(),
                            originalShuffleJoinOperator.getOnPredicate(), originalShuffleJoinOperator.getJoinHint(),
                            originalShuffleJoinOperator.getLimit(), originalShuffleJoinOperator.getPredicate(),
                            projectionOnJoin, skewSideJoinKeyExpr, nonNullSkewValues);

            // we have to let them know each other for runtime filter
            newBroadcastJoinOpt.setSkewJoinFriend(newShuffleJoinOpt);
            newShuffleJoinOpt.setSkewJoinFriend(newBroadcastJoinOpt);

            LocalExchangerType localExchangerType =
                    opt.isExistRequiredDistribution() ? LocalExchangerType.PASS_THROUGH : LocalExchangerType.DIRECT;
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
                            List.of(skewSideSplit.splitConsumerOptForShuffleJoin,
                                    nonSkewSideSplit.splitConsumerOptForShuffleJoin))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setRequiredProperties(opt.getRequiredProperties()).setCost(opt.getCost()).build();

            PhysicalPropertySet rightBroadcastProperty = new PhysicalPropertySet(
                    DistributionProperty.createProperty(DistributionSpec.createReplicatedDistributionSpec()));
            List<PhysicalPropertySet> requiredPropertiesForBroadcastJoin =
                    Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty);
            OptExpression newBroadcastJoin = OptExpression.builder().setOp(newBroadcastJoinOpt).setInputs(
                            List.of(skewSideSplit.splitConsumerOptForBroadcastJoin,
                                    nonSkewSideSplit.splitConsumerOptForBroadcastJoin))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setRequiredProperties(requiredPropertiesForBroadcastJoin).setCost(opt.getCost()).build();

            OptExpression rightChildOfConcatenate = newBroadcastJoin;
            if (opt.isExistRequiredDistribution()) {
                PhysicalDistributionOperator rightExchangeOpOfOriginalShuffleJoin =
                        (PhysicalDistributionOperator) nonSkewSideChild.getOp();

                PhysicalDistributionOperator newExchangeForBroadcastJoin =
                        new PhysicalDistributionOperator(rightExchangeOpOfOriginalShuffleJoin.getDistributionSpec());
                // we need add exchange node to make broadcast join's output distribution can be same as shuffle join's
                rightChildOfConcatenate = OptExpression.builder().setOp(newExchangeForBroadcastJoin)
                        .setInputs(Collections.singletonList(newBroadcastJoin))
                        .setLogicalProperty(newBroadcastJoin.getLogicalProperty())
                        .setStatistics(newBroadcastJoin.getStatistics()).setCost(newBroadcastJoin.getCost())
                        .build();
                if (projectionOnJoin != null) {
                    // broadcast join's projection should keep the columns used in exchange
                    Projection projectionForBroadcast = projectionOnJoin.deepClone();

                    ColumnRefSet usedColumnsOfRightExchangeOp =
                            rightExchangeOpOfOriginalShuffleJoin.getRowOutputInfo(nonSkewSideChild.getInputs())
                                    .getUsedColumnRefSet();
                    usedColumnsOfRightExchangeOp.except(projectionOnJoin.getOutputColumns());
                    usedColumnsOfRightExchangeOp.getColumnRefOperators(columnRefFactory)
                            .forEach(columnRefOperator -> {
                                projectionForBroadcast.getColumnRefMap()
                                        .putIfAbsent(columnRefOperator, columnRefOperator);
                            });
                    newBroadcastJoinOpt.setProjection(projectionForBroadcast);
                }
            }

            OptExpression concatenateOptExp = OptExpression.builder().setOp(concatenateOperator)
                    .setInputs(List.of(newShuffleJoin, rightChildOfConcatenate))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp1 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                            .setInputs(List.of(nonSkewSideSplit.splitProducer, concatenateOptExp))
                            .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            // if hit once, we give up rewriting the following subtree
            return OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                    .setInputs(List.of(skewSideSplit.splitProducer, cteAnchorOptExp1))
                    .setLogicalProperty(opt.getLogicalProperty()).setStatistics(opt.getStatistics())
                    .setCost(opt.getCost()).build();
        }

        private SplitProducerAndConsumer generateSplitProducerAndConsumer(OptExpression exchangeOptExp,
                                                                          ScalarOperator skewColumn,
                                                                          List<ScalarOperator> skewValues,
                                                                          boolean includeNullSkew,
                                                                          boolean isLeft) {
            PhysicalDistributionOperator exchangeOpOfOriginalShuffleJoin =
                    (PhysicalDistributionOperator) exchangeOptExp.getOp();

            PhysicalSplitProduceOperator splitProduceOperator =
                    new PhysicalSplitProduceOperator(uniqueSplitId.getAndIncrement());
            OptExpression splitProduceOptExp =
                    OptExpression.builder().setOp(splitProduceOperator).setInputs(exchangeOptExp.getInputs())
                            .setLogicalProperty(exchangeOptExp.getLogicalProperty())
                            .setStatistics(exchangeOptExp.getStatistics()).setCost(exchangeOptExp.getCost()).build();

            Pair<ScalarOperator, ScalarOperator> tablePredicates =
                    generateSkewAndNonSkewPredicate(skewColumn, skewValues, includeNullSkew);

            List<ColumnRefOperator> splitOutputColumns =
                    exchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory);

            // for broadcast join, left table skew values are sent by round-robin way
            // right table skew values are sent by broadcast
            DistributionSpec distributionSpecForBroadCastJoin;
            if (isLeft) {
                distributionSpecForBroadCastJoin = new RoundRobinDistributionSpec();
            } else {
                distributionSpecForBroadCastJoin = DistributionSpec.createReplicatedDistributionSpec();
            }

            // use Not-In predicate for shuffle join's input, since shuffle join handle non-skew data
            PhysicalSplitConsumeOperator splitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(splitProduceOperator.getSplitId(), tablePredicates.second,
                            exchangeOpOfOriginalShuffleJoin.getDistributionSpec(), splitOutputColumns);

            //  use In predicate for broadcast join's input, since broadcast join handle skew data
            PhysicalSplitConsumeOperator splitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(splitProduceOperator.getSplitId(), tablePredicates.first,
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

            ScalarOperator isNullPredicate = new IsNullPredicateOperator(skewColumn);

            // Skew predicate (broadcast-join branch)
            ScalarOperator skewPredicate;
            if (includeNullSkew) {
                skewPredicate = (inSkewPredicate == null) ? isNullPredicate
                        : CompoundPredicateOperator.or(inSkewPredicate, isNullPredicate);
            } else {
                if (inSkewPredicate == null) {
                    throw new IllegalStateException("skewValues is empty while includeNullSkew is false");
                }
                skewPredicate = inSkewPredicate;
            }

            // Non-skew predicate (shuffle-join branch)
            ScalarOperator nonSkewPredicate;
            if (includeNullSkew) {
                ScalarOperator isNotNullPredicate = new IsNullPredicateOperator(true, skewColumn);
                if (inSkewPredicate == null) {
                    nonSkewPredicate = isNotNullPredicate;
                } else {
                    ScalarOperator notInSkewPredicate = new InPredicateOperator(true, inSkewPredicate.getChildren());
                    nonSkewPredicate = CompoundPredicateOperator.and(notInSkewPredicate, isNotNullPredicate);
                }
            } else {
                ScalarOperator notInSkewPredicate = new InPredicateOperator(true, inSkewPredicate.getChildren());
                nonSkewPredicate = CompoundPredicateOperator.or(notInSkewPredicate, isNullPredicate);
            }

            return Pair.create(skewPredicate, nonSkewPredicate);
        }

        private boolean isExchangeWithDistributionType(Operator child) {
            if (!(child.getOpType() == OperatorType.PHYSICAL_DISTRIBUTION)) {
                return false;
            }
            PhysicalDistributionOperator operator = child.cast();
            return Objects.equals(operator.getDistributionSpec().getType(), DistributionSpec.DistributionType.SHUFFLE);
        }

        private PhysicalConcatenateOperator buildConcatenateOperator(List<ColumnRefOperator> outputColumns,
                                                                     LocalExchangerType localExchangeType,
                                                                     long limit) {
            List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                childOutputColumns.add(outputColumns);
            }
            return new PhysicalConcatenateOperator(outputColumns, childOutputColumns, localExchangeType, limit);
        }

        private SkewJoinSplitInfo findSkewJoinSplitInfo(OptExpression input,
                                                        ColumnRefOperator skewColumnRef,
                                                        List<ScalarOperator> nonNullSkewValues,
                                                        boolean includeNullSkew) {
            ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();
            if (leftOutputColumns.contains(skewColumnRef)) {
                return findSkewJoinSplitInfo(input, skewColumnRef, 0, nonNullSkewValues, includeNullSkew);
            } else if (rightOutputColumns.contains(skewColumnRef)) {
                return findSkewJoinSplitInfo(input, skewColumnRef, 1, nonNullSkewValues, includeNullSkew);
            } else {
                // for join's on predicates contain expressions with skew column, try left and right sides both.
                // try left side first
                SkewJoinSplitInfo result = findSkewJoinSplitInfo(input, skewColumnRef, 0, nonNullSkewValues, includeNullSkew);
                if (result.skewSideJoinKeyExpr != null && result.nonSkewSideJoinKeyExpr != null) {
                    return result;
                }
                // then try right side
                return findSkewJoinSplitInfo(input, skewColumnRef, 1, nonNullSkewValues, includeNullSkew);
            }
        }

        private SkewJoinSplitInfo findSkewJoinSplitInfo(OptExpression input,
                                                        ColumnRefOperator skewColumnRef,
                                                        int skewSideChildIndex,
                                                        List<ScalarOperator> nonNullSkewValues,
                                                        boolean includeNullSkew) {
            PhysicalHashJoinOperator joinOperator = input.getOp().cast();
            ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();
            ScalarOperator skewSideJoinKeyExpr = null;
            ScalarOperator nonSkewSideJoinKeyExpr = null;
            List<ScalarOperator> rewrittenNonNullSkewValues = nonNullSkewValues;

            OptExpression skewSideChild = input.inputAt(skewSideChildIndex);
            // get equal conjuncts from on predicate, every eq predicate's child can't be constant
            List<BinaryPredicateOperator> equalConjs =
                    JoinHelper.getEqualsPredicate(leftOutputColumns, rightOutputColumns,
                            Utils.extractConjuncts(joinOperator.getOnPredicate()));
            Projection skewSideProject = skewSideChild.getInputs().isEmpty() ? null :
                    skewSideChild.inputAt(0).getOp().getProjection();
            for (BinaryPredicateOperator equalConj : equalConjs) {
                ScalarOperator child0 = equalConj.getChild(0);
                ScalarOperator child1 = equalConj.getChild(1);

                // Find the join key expressions on skew side / non-skew side.
                // If one side of the equality uses the skew column (or an expression derived from it),
                // the other side is the corresponding join key expression on the opposite child.
                //
                // Example:
                //   SELECT ... FROM t1 JOIN[skew|t1.c_tinyint(1,2,3)] t2 ON t1.c_tinyint = t2.c_int
                // After type coercion, on predicate may become:
                //   cast(t1.c_tinyint as int) = t2.c_int
                // In this case we need to rewrite non-null skew values as:
                //   cast(1 as int), cast(2 as int), cast(3 as int)
                if (child0.equals(skewColumnRef)) {
                    skewSideJoinKeyExpr = skewColumnRef;
                    nonSkewSideJoinKeyExpr = child1;
                } else if (child1.equals(skewColumnRef)) {
                    skewSideJoinKeyExpr = skewColumnRef;
                    nonSkewSideJoinKeyExpr = child0;
                } else {
                    // originalSkewColumn is part of some expr
                    if (skewSideProject != null) {
                        ScalarOperator rewriteChild0 = replaceColumnRef(child0, skewSideProject);
                        ScalarOperator rewriteChild1 = replaceColumnRef(child1, skewSideProject);
                        if (rewriteChild0.getUsedColumns().contains(skewColumnRef)) {
                            skewSideJoinKeyExpr = child0;
                            nonSkewSideJoinKeyExpr = child1;
                        } else if (rewriteChild1.getUsedColumns().contains(skewColumnRef)) {
                            nonSkewSideJoinKeyExpr = child0;
                            skewSideJoinKeyExpr = child1;
                        } else {
                            continue;
                        }

                        if (skewSideJoinKeyExpr instanceof ColumnRefOperator skewSideJoinKeyRef) {
                            Map<ColumnRefOperator, ScalarOperator> skewSideProjectMap = skewSideProject.getAllMaps();
                            ScalarOperator definingExpr = skewSideProjectMap.get(skewSideJoinKeyRef);
                            if (definingExpr != null && definingExpr.getUsedColumns().contains(skewColumnRef)) {
                                rewrittenNonNullSkewValues =
                                        rewriteSkewValues(rewrittenNonNullSkewValues, definingExpr, skewColumnRef);
                            }
                        }
                    }
                }
            }

            return new SkewJoinSplitInfo(rewrittenNonNullSkewValues,
                    skewSideJoinKeyExpr,
                    nonSkewSideJoinKeyExpr,
                    skewSideChildIndex,
                    includeNullSkew);
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
                                                       ScalarOperator predicate,
                                                       ColumnRefOperator targetColumnRef) {
            List<ScalarOperator> result = new LinkedList<>();
            // For each skew value, replace only the target column ref with that constant value.
            originalSkewValues.forEach(value -> {
                ColumnRefReplacer refReplacer = new ColumnRefReplacer(targetColumnRef, value);
                ScalarOperator newSkewValue = predicate.accept(refReplacer, null);
                result.add(newSkewValue);
            });
            return result;
        }

        private boolean canOptimize(PhysicalHashJoinOperator joinOp, OptExpression opt) {
            return isValidJoinType(joinOp) && isShuffleJoin(opt);
        }

        // currently only support inner join and left outer join
        private boolean isValidJoinType(PhysicalHashJoinOperator joinOp) {
            return joinOp.getJoinType().isAnyInnerJoin() || joinOp.getJoinType().isAnyLeftOuterJoin();
        }

        private boolean isShuffleJoin(OptExpression opt) {
            if (opt.getOp().getOpType() != OperatorType.PHYSICAL_HASH_JOIN) {
                return false;
            }
            return isExchangeWithDistributionType(opt.getInputs().get(0).getOp()) &&
                    isExchangeWithDistributionType(opt.getInputs().get(1).getOp());
        }

        // Hint-based skew join (manual).
        private boolean isSkew(OptExpression opt) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) opt.getOp();

            if (joinOperator.getSkewColumn() == null || joinOperator.getSkewValues() == null ||
                    joinOperator.getSkewValues().isEmpty()) {
                return false;
            }

            // respect join hint
            return joinOperator.getJoinHint().equals(HintNode.HINT_JOIN_SKEW);
        }

        private Optional<SkewJoinSplitInfo> detectSkewFromStats(OptExpression joinOpt) {
            if (sessionVariable == null || !sessionVariable.isEnableStatsToOptimizeSkewJoin()) {
                return Optional.empty();
            }
            PhysicalHashJoinOperator joinOperator = joinOpt.getOp().cast();

            ColumnRefSet leftOutputColumns = joinOpt.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = joinOpt.inputAt(1).getOutputColumns();
            List<BinaryPredicateOperator> equalConjs =
                    JoinHelper.getEqualsPredicate(leftOutputColumns, rightOutputColumns,
                            Utils.extractConjuncts(joinOperator.getOnPredicate()));
            if (equalConjs.size() != 1) {
                return Optional.empty();
            }

            BinaryPredicateOperator eq = equalConjs.get(0);
            if (!eq.getChild(0).isColumnRef() || !eq.getChild(1).isColumnRef()) {
                return Optional.empty();
            }

            ColumnRefOperator c0 = eq.getChild(0).cast();
            ColumnRefOperator c1 = eq.getChild(1).cast();
            ColumnRefOperator leftKey = leftOutputColumns.contains(c0) ? c0 :
                    (leftOutputColumns.contains(c1) ? c1 : null);
            ColumnRefOperator rightKey = (leftKey == null) ? null : (leftKey.equals(c0) ? c1 : c0);
            if (leftKey == null || rightKey == null) {
                return Optional.empty();
            }

            double bestScore = -1.0;
            SkewJoinSplitInfo best = null;
            for (int side = 0; side < 2; side++) {
                if (joinOperator.getJoinType().isAnyLeftOuterJoin() && side == 1) {
                    continue;
                }
                ColumnRefOperator skewSideKeyColumn = (side == 0) ? leftKey : rightKey;
                Statistics stats = joinOpt.inputAt(side).getStatistics();
                if (stats == null || stats.getOutputRowCount() < sessionVariable.getSkewJoinMcvMinInputRows()) {
                    continue;
                }

                if (!stats.getColumnStatistics().containsKey(skewSideKeyColumn)) {
                    continue;
                }
                ColumnStatistic cs = stats.getColumnStatistic(skewSideKeyColumn);
                if (cs.isUnknown() || cs.getHistogram() == null) {
                    continue;
                }

                DataSkew.Thresholds thresholds = new DataSkew.Thresholds(
                        sessionVariable.getSkewJoinOptimizeUseMCVCount(),
                        sessionVariable.getSkewJoinDataSkewThreshold());
                DataSkew.SkewCandidates candidates =
                        DataSkew.getSkewCandidates(stats, cs, thresholds, sessionVariable.getSkewJoinMcvSingleThreshold());
                if (!candidates.isSkewed()) {
                    continue;
                }

                // Pick the side with larger skew factor.
                double score = Math.max(candidates.nullSkewFactor().orElse(0.0),
                        candidates.mcvSkewFactor().orElse(0.0));
                if (score <= bestScore) {
                    continue;
                }

                List<ScalarOperator> nonNullValues = new ArrayList<>();
                for (Pair<String, Long> p : candidates.mcvs()) {
                    ConstantOperator keyConst = ConstantOperator.createVarchar(p.first);
                    keyConst.castTo(skewSideKeyColumn.getType()).ifPresent(nonNullValues::add);
                }
                boolean includeNullSkew = candidates.includeNull();
                if (nonNullValues.isEmpty() && !includeNullSkew) {
                    continue;
                }

                SkewJoinSplitInfo mapped = findSkewJoinSplitInfo(joinOpt, skewSideKeyColumn, nonNullValues, includeNullSkew);
                bestScore = score;
                best = mapped;
            }

            return Optional.ofNullable(best);
        }

        private static class ColumnRefReplacer extends BaseScalarOperatorShuttle {

            private final ColumnRefOperator targetColumnRef;
            private final ScalarOperator scalarOperator;

            public ColumnRefReplacer(ColumnRefOperator targetColumnRef, ScalarOperator scalarOperator) {
                this.targetColumnRef = targetColumnRef;
                this.scalarOperator = scalarOperator;
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                if (targetColumnRef != null && targetColumnRef.equals(variable)) {
                    return scalarOperator;
                }
                return variable;
            }
        }
    }
}
