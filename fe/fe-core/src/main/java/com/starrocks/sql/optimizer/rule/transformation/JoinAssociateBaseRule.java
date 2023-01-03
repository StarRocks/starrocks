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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.RowDescriptor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnEntry;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

public abstract class JoinAssociateBaseRule extends TransformationRule {

    protected static final List<int[]> ASSOCIATE_MODE = ImmutableList
            .of(new int[] {0, 0}, new int[] {0, 1}, new int[] {1, -1});

    protected static final List<int[]> LEFTASSCOM_MODE = ImmutableList
            .of(new int[] {0, 1}, new int[] {0, 0}, new int[] {1, -1});

    protected final int[] newTopJoinChildLoc;

    protected final int[] newBotJoinLeftChildLoc;

    protected final int[] newBotJoinRightChildLoc;

    protected JoinAssociateBaseRule(RuleType type, Pattern pattern, List<int[]> mode) {
        super(type, pattern);
        this.newTopJoinChildLoc = mode.get(0);
        this.newBotJoinLeftChildLoc = mode.get(1);
        this.newBotJoinRightChildLoc = mode.get(2);
    }

    public abstract ScalarOperator rewriteNewTopOnCondition(JoinOperator topJoinType, ProjectionSplitter splitter,
                                                  ScalarOperator newTopOnCondition,
                                                  ColumnRefSet newBotJoinOutputCols,
                                                  ColumnRefFactory columnRefFactory);

    public abstract OptExpression createNewTopJoinExpr(LogicalJoinOperator newTopJoin, OptExpression newTopJoinChild,
                                                       OptExpression newBotJoinExpr);

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator.Builder newTopJoinBuilder = new LogicalJoinOperator.Builder();
        LogicalJoinOperator.Builder newBottomJoinBuilder = new LogicalJoinOperator.Builder();

        OptExpression bottomJoinExpr = input.inputAt(0);
        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator bottomJoin = (LogicalJoinOperator) bottomJoinExpr.getOp();

        List<ScalarOperator> top = Lists.newArrayList();
        List<ScalarOperator> bottom = Lists.newArrayList();

        // pull up bottom join predicate
        ScalarOperator topPredicate = topJoin.getPredicate();
        ScalarOperator newTopPredicate = Utils.compoundAnd(bottomJoin.getPredicate(), topPredicate);

        ColumnRefSet newTopJoinChildOutCols = deriveTopJoinChildOutputCols(input);

        splitCondition(topJoin.getOnPredicate(), newTopJoinChildOutCols, top, bottom);
        // bottom join on condition doesn't need to split, it can be pulled up to the top directly.
        top.addAll(Utils.extractConjuncts(bottomJoin.getOnPredicate()));

        ScalarOperator newTopOnCondition = Utils.compoundAnd(top);
        ScalarOperator newBotOnCondition = Utils.compoundAnd(bottom);

        ColumnRefSet newBotJoinColSet = deriveNewBotJoinColSet(input.getRowDescriptor(), newTopOnCondition,
                newTopPredicate, newTopJoinChildOutCols);

        JoinOperator newTopJoinType = deriveJoinType(newTopOnCondition, bottomJoin.getJoinType());
        JoinOperator newBotJoinType = deriveJoinType(newBotOnCondition, topJoin.getJoinType());

        // split bottomJoin project
        ProjectionSplitter splitter = new ProjectionSplitter(input, newBotOnCondition);

        newTopOnCondition = rewriteNewTopOnCondition(newTopJoinType, splitter, newTopOnCondition,
                newBotJoinColSet, context.getColumnRefFactory());

        newBotOnCondition = rewriteNewBotOnCondition(splitter, newBotOnCondition);

        OptExpression newTopJoinChild = input.inputAt(newTopJoinChildLoc[0]).inputAt(newTopJoinChildLoc[1]);

        if (CollectionUtils.isNotEmpty(splitter.getTopJoinChildCols())) {
            RowDescriptor mergedRow = newTopJoinChild.getRowDescriptor().addColsToRow(
                    splitter.getTopJoinChildCols(),
                    newTopJoinChild.getOp().getProjection() != null);
            Operator.Builder builder = OperatorBuilderFactory.build(newTopJoinChild.getOp());

            Operator childOp = builder.withOperator(newTopJoinChild.getOp())
                    .setProjection(new Projection(mergedRow.getColumnRefMap())).build();
            newTopJoinChild = OptExpression.create(childOp, newTopJoinChild.getInputs());
        }

        OptExpression newBotJoinLeftChild = input.inputAt(newBotJoinLeftChildLoc[0]).inputAt(newBotJoinLeftChildLoc[1]);
        OptExpression newBotJoinRightChild = input.inputAt(newBotJoinRightChildLoc[0]);

        if (CollectionUtils.isNotEmpty(splitter.getBotJoinChildCols())) {
            RowDescriptor mergedRow = newBotJoinLeftChild.getRowDescriptor().addColsToRow(
                    splitter.getBotJoinChildCols(),
                    newBotJoinLeftChild.getOp().getProjection() != null);
            Operator.Builder builder = OperatorBuilderFactory.build(newBotJoinLeftChild.getOp());

            Operator newBotJoinLeftChildOp = builder.withOperator(newBotJoinLeftChild.getOp())
                    .setProjection(new Projection(mergedRow.getColumnRefMap())).build();
            newBotJoinLeftChild = OptExpression.create(newBotJoinLeftChildOp, newBotJoinLeftChild.getInputs());
        }

        RowDescriptor newBotJoinRowInfo = deriveBotJoinRowInfo(newBotJoinColSet, newBotJoinLeftChild,
                newBotJoinRightChild, splitter);

        Projection newBotJoinProjection = null;
        if (needProject(newBotJoinRowInfo, newBotJoinLeftChild, newBotJoinRightChild)) {
            newBotJoinProjection = new Projection(newBotJoinRowInfo.getColumnRefMap());
        }

        LogicalJoinOperator newBotJoin = newBottomJoinBuilder.setJoinType(newBotJoinType)
                .setRowDescriptor(newBotJoinRowInfo)
                .setOnPredicate(newBotOnCondition)
                .setProjection(newBotJoinProjection)
                .build();
        OptExpression newBotJoinExpr = OptExpression.create(newBotJoin, newBotJoinLeftChild, newBotJoinRightChild);

        Projection newTopJoinProjection = null;

        if (needProject(input.getRowDescriptor(), newTopJoinChild, newBotJoinExpr)) {
            newTopJoinProjection = new Projection(input.getRowDescriptor().getColumnRefMap());
        }

        LogicalJoinOperator newTopJoin = newTopJoinBuilder.withOperator(topJoin)
                .setJoinType(newTopJoinType)
                .setRowDescriptor(input.getRowDescriptor())
                .setProjection(newTopJoinProjection)
                .setOnPredicate(newTopOnCondition)
                .setPredicate(newTopPredicate)
                .build();

        OptExpression newTopJoinExpr = createNewTopJoinExpr(newTopJoin, newTopJoinChild, newBotJoinExpr);
        
        return Lists.newArrayList(newTopJoinExpr);
    }

    private boolean invalidPlan(OptExpression newTopJoinExpr) {
        ColumnRefSet requiredCols = ((LogicalJoinOperator) newTopJoinExpr.getOp()).getRequiredCols();
        ColumnRefSet left = newTopJoinExpr.inputAt(0).getRowDescriptor().getOutputColumnRefSet();
        ColumnRefSet right = newTopJoinExpr.inputAt(1).getRowDescriptor().getOutputColumnRefSet();
        requiredCols.except(left);
        requiredCols.except(right);
        return !requiredCols.isEmpty();
    }

    protected ColumnRefSet deriveTopJoinChildOutputCols(OptExpression input) {
        OptExpression newTopJoinChildOpt = input.inputAt(newTopJoinChildLoc[0]).inputAt(newTopJoinChildLoc[1]);
        RowDescriptor oldBotJoinOutput = input.inputAt(0).getRowDescriptor();
        ColumnRefSet cols = newTopJoinChildOpt.getRowDescriptor().getOutputColumnRefSet();
        for (ColumnEntry entry : oldBotJoinOutput.getColumnEntries()) {
            if (entry.getUsedColumns().isIntersect(cols)) {
                cols.union(entry.getColumnRef());
            }
        }
        return cols;
    }

    protected ColumnRefSet deriveNewBotJoinColSet(RowDescriptor topRow, ScalarOperator onCondition,
                                                  ScalarOperator predicate, ColumnRefSet columnRefSet) {
        ColumnRefSet requiredCols = topRow.getUsedColumnRefSet();
        if (onCondition != null) {
            requiredCols.union(onCondition.getUsedColumns());
        }
        if (predicate != null) {
            requiredCols.union(predicate.getUsedColumns());

        }
        ColumnRefSet result = requiredCols.clone();
        result.except(columnRefSet);
        return result;
    }

    protected RowDescriptor deriveBotJoinRowInfo(ColumnRefSet columnRefSet, OptExpression leftChild,
                                           OptExpression rightChild, ProjectionSplitter splitter) {
        List<ColumnEntry> columnEntries = Lists.newArrayList();

        if (columnRefSet.isEmpty()) {
            ColumnEntry anyCol = leftChild.getRowDescriptor().getColumnEntries().get(0);
            columnEntries.add(new ColumnEntry(anyCol.getColumnRef(), anyCol.getColumnRef()));
        } else {
            for (ColumnEntry entry : leftChild.getRowDescriptor().getColumnEntries()) {
                if (columnRefSet.contains(entry.getColId())) {
                    columnEntries.add(new ColumnEntry(entry.getColumnRef(), entry.getColumnRef()));
                }
            }

            for (ColumnEntry entry : rightChild.getRowDescriptor().getColumnEntries()) {
                if (columnRefSet.contains(entry.getColId())) {
                    columnEntries.add(new ColumnEntry(entry.getColumnRef(), entry.getColumnRef()));
                }
            }

            columnEntries.addAll(splitter.getBotJoinCols());
            columnEntries.addAll(splitter.getConstCols());
        }
        return new RowDescriptor(columnEntries);
    }

    protected boolean needProject(RowDescriptor row, OptExpression oneChild, OptExpression otherChild) {
        ColumnRefSet outputRowCols = row.getOutputColumnRefSet();
        ColumnRefSet inputRowCols = new ColumnRefSet();
        inputRowCols.union(oneChild.getRowDescriptor().getOutputColumnRefSet());
        inputRowCols.union(otherChild.getRowDescriptor().getOutputColumnRefSet());
        return !outputRowCols.equals(inputRowCols);
    }

    protected void splitCondition(ScalarOperator onCondition, ColumnRefSet columnRefSet,
            List<ScalarOperator> intersect, List<ScalarOperator> nonIntersect) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(onCondition);
        for (ScalarOperator conjunct : conjuncts) {
            if (columnRefSet.isIntersect(conjunct.getUsedColumns())) {
                intersect.add(conjunct);
            } else {
                nonIntersect.add(conjunct);
            }
        }
    }

    protected JoinOperator deriveJoinType(ScalarOperator onCondition, JoinOperator joinType) {
        if (onCondition != null && joinType.isCrossJoin()) {
            return JoinOperator.INNER_JOIN;
        } else {
            return joinType;
        }
    }

    protected ScalarOperator rewriteNewBotOnCondition(ProjectionSplitter splitter, ScalarOperator newBotOnCondition) {
        if (newBotOnCondition == null || CollectionUtils.isEmpty(splitter.getConstCols())) {
            return newBotOnCondition;
        }
        Map<ColumnRefOperator, ScalarOperator> colRefMap = Maps.newHashMap();
        splitter.getConstCols().stream().forEach(e -> colRefMap.put(e.getColumnRef(), e.getScalarOp()));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(colRefMap);
        newBotOnCondition = rewriter.rewrite(newBotOnCondition);
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        newBotOnCondition = scalarRewriter.rewrite(newBotOnCondition, ImmutableList.of(new NormalizePredicateRule()));
        return newBotOnCondition;

    }

    protected class ProjectionSplitter {
        // save columnEntry belongs to new topJoin child
        List<ColumnEntry> topJoinChildCols = Lists.newArrayList();

        // save columnEntry belongs to the projection of new botJoin
        List<ColumnEntry> botJoinCols = Lists.newArrayList();

        // save columnEntry belongs to the projection of new botJoin's child
        List<ColumnEntry> botJoinChildCols = Lists.newArrayList();

        // save columnEntry which is constant
        List<ColumnEntry> constCols = Lists.newArrayList();



        public ProjectionSplitter(OptExpression input, ScalarOperator newBotJoinOnCondition) {
            RowDescriptor rowDescriptor = input.inputAt(0).getRowDescriptor();
            OptExpression newBotJoinLeftChildOpt = input.
                    inputAt(newBotJoinLeftChildLoc[0]).inputAt(newBotJoinLeftChildLoc[1]);
            if (input.inputAt(0).getOp().getProjection() == null) {
                return;
            }
            ColumnRefSet leftChildCols = newBotJoinLeftChildOpt.getRowDescriptor().getOutputColumnRefSet();
            for (ColumnEntry columnEntry : rowDescriptor.getColumnEntries()) {
                ColumnRefOperator columnRef = columnEntry.getColumnRef();
                ScalarOperator scalarOp = columnEntry.getScalarOp();
                if (!columnRef.equals(scalarOp)) {
                    if (scalarOp.getUsedColumns().isEmpty()) {
                        constCols.add(columnEntry);
                    } else if (leftChildCols.containsAll(scalarOp.getUsedColumns())) {
                        if (needPushToChild(newBotJoinOnCondition, columnEntry)) {
                            botJoinChildCols.add(columnEntry);
                        } else {
                            botJoinCols.add(columnEntry);
                        }

                    } else {
                        topJoinChildCols.add(columnEntry);
                    }
                }
            }
        }

        private boolean needPushToChild(ScalarOperator newBotJoinOnCondition, ColumnEntry columnEntry) {
            if (newBotJoinOnCondition == null) {
                return false;
            }

            return newBotJoinOnCondition.getUsedColumns().contains(columnEntry.getColumnRef());
        }

        public List<ColumnEntry> getTopJoinChildCols() {
            return topJoinChildCols;
        }

        public List<ColumnEntry> getBotJoinCols() {
            return botJoinCols;
        }

        public List<ColumnEntry> getBotJoinChildCols() {
            return botJoinChildCols;
        }

        public List<ColumnEntry> getConstCols() {
            return constCols;
        }
    }

}
