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
import com.starrocks.sql.optimizer.operator.physical.PhysicalConcatenateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
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
    private AtomicInteger uniqueSplitId;

    private SkewShuffleJoinEliminationVisitor handler = null;

    public SkewShuffleJoinEliminationRule() {
        this.uniqueSplitId = new AtomicInteger();
    }

    class SkewColumnAndValues {
        public SkewColumnAndValues(List<ScalarOperator> skewValues,
                                   Pair<ScalarOperator, ScalarOperator> skewColumns) {
            this.skewValues = skewValues;
            this.skewColumns = skewColumns;
        }

        List<ScalarOperator> skewValues;
        Pair<ScalarOperator, ScalarOperator> skewColumns;
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        handler = new SkewShuffleJoinEliminationVisitor(taskContext.getOptimizerContext().getColumnRefFactory());
        return root.getOp().accept(handler, root, null);
    }

    private class SkewShuffleJoinEliminationVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private ColumnRefFactory columnRefFactory;

        public SkewShuffleJoinEliminationVisitor(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Void Context) {
            return visitChild(optExpr, Context);
        }

        private OptExpression visitChild(OptExpression opt, Void Context) {
            for (int idx = 0; idx < opt.arity(); ++idx) {
                OptExpression child = opt.inputAt(idx);
                opt.setChild(idx, child.getOp().accept(this, child, null));
            }
            return opt;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression opt, Void Context) {
            PhysicalHashJoinOperator originalShuffleJoinOperator = (PhysicalHashJoinOperator) opt.getOp();

            if (!canOptimize(originalShuffleJoinOperator, opt)) {
                return visitChild(opt, null);
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
            OptExpression leftExchangeOptExp = opt.inputAt(0);
            PhysicalDistributionOperator leftExchangeOpOfOriginalShuffleJoin =
                    (PhysicalDistributionOperator) leftExchangeOptExp.getOp();

            OptExpression rightExchangeOptExp = opt.inputAt(1);
            PhysicalDistributionOperator rightExchangeOpOfOriginalShuffleJoin =
                    (PhysicalDistributionOperator) rightExchangeOptExp.getOp();

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

            Pair<ScalarOperator, ScalarOperator> leftTablePredicates =
                    generateInAndNotInPredicate(leftSkewColumn, skewValues);
            Pair<ScalarOperator, ScalarOperator> rightTablePredicates =
                    generateInAndNotInPredicate(rightSkewColumn, skewValues);

            List<ColumnRefOperator> leftSplitOutputColumns =
                    leftExchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory);
            List<ColumnRefOperator> rightSplitOutputColumns =
                    rightExchangeOptExp.getOutputColumns().getColumnRefOperators(columnRefFactory);

            PhysicalSplitConsumeOperator leftSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(),
                            leftTablePredicates.second,
                            leftExchangeOpOfOriginalShuffleJoin.getDistributionSpec(), leftSplitOutputColumns);

            PhysicalSplitConsumeOperator leftSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(leftSplitProduceOperator.getSplitId(), leftTablePredicates.first,
                            new RoundRobinDistributionSpec(), leftSplitOutputColumns);

            PhysicalSplitConsumeOperator rightSplitConsumerOptForShuffleJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(),
                            rightTablePredicates.second,
                            rightExchangeOpOfOriginalShuffleJoin.getDistributionSpec(), rightSplitOutputColumns);

            PhysicalSplitConsumeOperator rightSplitConsumerOptForBroadcastJoin =
                    new PhysicalSplitConsumeOperator(rightSplitProduceOperator.getSplitId(),
                            rightTablePredicates.first,
                            DistributionSpec.createReplicatedDistributionSpec(), rightSplitOutputColumns);

            Projection projectionOnJoin = originalShuffleJoinOperator.getProjection();

            PhysicalHashJoinOperator newShuffleJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), projectionOnJoin,
                    leftSkewColumn, skewValues);

            PhysicalHashJoinOperator newBroadcastJoinOpt = new PhysicalHashJoinOperator(
                    originalShuffleJoinOperator.getJoinType(), originalShuffleJoinOperator.getOnPredicate(),
                    originalShuffleJoinOperator.getJoinHint(), originalShuffleJoinOperator.getLimit(),
                    originalShuffleJoinOperator.getPredicate(), projectionOnJoin,
                    leftSkewColumn, skewValues);


            LocalExchangerType localExchangerType =
                    opt.isExistRequiredDistribution() ? LocalExchangerType.PASS_THROUGH : LocalExchangerType.DIRECT;
            PhysicalConcatenateOperator concatenateOperator =
                    buildConcatenateOperator(opt.getOutputColumns().getColumnRefOperators(columnRefFactory), 2,
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

            OptExpression rightChildOfConcatenate = newBroadcastJoin;
            if (opt.isExistRequiredDistribution()) {
                PhysicalDistributionOperator newExchangeForBroadcastJoin =
                        new PhysicalDistributionOperator(rightExchangeOpOfOriginalShuffleJoin.getDistributionSpec());
                // we need add exchange node to make broadcast join's output distribution can be same as shuffle join's
                OptExpression exchangeOptExpForBroadcastJoin =
                        OptExpression.builder().setOp(newExchangeForBroadcastJoin)
                                .setInputs(Collections.singletonList(newBroadcastJoin))
                                .setLogicalProperty(newBroadcastJoin.getLogicalProperty())
                                .setStatistics(newBroadcastJoin.getStatistics())
                                .setCost(newBroadcastJoin.getCost()).build();
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
                    newBroadcastJoinOpt.setProjection(projectionForBroadcast);
                }
            }

            OptExpression concatenateOptExp =
                    OptExpression.builder().setOp(concatenateOperator)
                            .setInputs(List.of(newShuffleJoin, rightChildOfConcatenate))
                            .setLogicalProperty(opt.getLogicalProperty())
                            .setStatistics(opt.getStatistics())
                            .setCost(opt.getCost()).build();

            OptExpression cteAnchorOptExp1 =
                    OptExpression.builder().setOp(new PhysicalCTEAnchorOperator(uniqueSplitId.getAndIncrement()))
                            .setInputs(List.of(rightSplitProduceOptExp, concatenateOptExp))
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


        private Pair<ScalarOperator, ScalarOperator> generateInAndNotInPredicate(ScalarOperator skewColumn,
                                                                                 List<ScalarOperator> skewValues) {
            List<ScalarOperator> inPredicateParams = new ArrayList<>();
            inPredicateParams.add(skewColumn);
            inPredicateParams.addAll(skewValues);

            ScalarOperator inSkewPredicate = new InPredicateOperator(false, inPredicateParams);
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            inSkewPredicate = rewriter.rewrite(inSkewPredicate,
                    ImmutableList.of(new ImplicitCastRule(), new ReduceCastRule()));
            ScalarOperator notInSkewPredicate =
                    new InPredicateOperator(true, inSkewPredicate.getChildren());
            ScalarOperator notInSkewOrNullPredicate =
                    CompoundPredicateOperator.or(notInSkewPredicate,
                            new IsNullPredicateOperator(skewColumn));

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
                                                                     int childNum,
                                                               LocalExchangerType localExchangeType, long limit) {
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
            List<BinaryPredicateOperator> equalConjs = JoinHelper.
                    getEqualsPredicate(leftOutputColumns, rightOutputColumns,
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
                        if (leftColumnRefMap.keySet().contains(leftSkewColumn)) {
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

        private ScalarOperator replaceColumnRef(ScalarOperator scalarOperator,
                                                Projection projection) {
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

        private boolean canOptimize(PhysicalHashJoinOperator joinOp, OptExpression opt) {
            return isValidJoinType(joinOp) &&
                    isShuffleJoin(opt) &&
                    isSkew(opt);
        }

        // currently only support inner join and left outer join
        private boolean isValidJoinType(PhysicalHashJoinOperator joinOp) {
            return joinOp.getJoinType().isInnerJoin() ||
                    joinOp.getJoinType().isLeftOuterJoin();
        }

        private boolean isShuffleJoin(OptExpression opt) {
            if (opt.getOp().getOpType() != OperatorType.PHYSICAL_HASH_JOIN) {
                return false;
            }
            if (!isExchangeWithDistributionType(opt.getInputs().get(0).getOp(),
                    DistributionSpec.DistributionType.SHUFFLE) ||
                    !isExchangeWithDistributionType(opt.getInputs().get(1).getOp(),
                            DistributionSpec.DistributionType.SHUFFLE)) {
                return false;
            }
            return true;
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
            if (joinOperator.getJoinHint().equals(JoinOperator.HINT_SKEW)) {
                return true;
            }

            return false;
        }
    }

}
