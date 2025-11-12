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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.collect.Lists;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrLazyOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;

import java.util.List;

public class TvrJoinRule extends TvrTransformationRule {

    public TvrJoinRule() {
        super(RuleType.TF_TVR_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return isSupportedTvr(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        OptExpression leftChildDelta = input.inputAt(0);
        TvrOptMeta leftOptMeta = leftChildDelta.getTvrMeta();
        OptExpression rightChildDelta = input.inputAt(1);
        TvrOptMeta rightOptMeta = rightChildDelta.getTvrMeta();

        // TODO: use left table's tvrOptMeta as the root
        // TODO: Use mv as the history state instead of recomputing.
        List<ColumnRefOperator> joinOutputColRefs = input.getRowOutputInfo().getOutputColRefs();

        // delta join
        if (!leftOptMeta.isAppendOnly() || !rightOptMeta.isAppendOnly()) {
            throw new IllegalStateException("Join operator should be append-only for TVR: " + join.getJoinType()
                    + " in " + input);
        }
        OptExpression deltaJoin = doTransformWithMonotonic(context, input, join, joinOutputColRefs,
                leftOptMeta, rightOptMeta);

        return Lists.newArrayList(deltaJoin);
    }

    private TvrOptMeta buildJoinTvrOptMeta(OptimizerContext context,
                                           LogicalJoinOperator join,
                                           TvrOptMeta leftOptMeta,
                                           TvrOptMeta rightOptMeta,
                                           List<ColumnRefOperator> joinOutputColRefs,
                                           TvrTableDeltaTrait joinDeltaTrait) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        // from opt
        TvrLazyOptExpression fromJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput fromOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftFrom.optExpression(), tvrRightFrom.optExpression(), false);
            return new TvrOptExpression(tvrLeftFrom.tvrVersionRange(), fromOpt.optExpression());
        });
        // to opt
        TvrLazyOptExpression toJoin = TvrLazyOptExpression.of(() -> {
            LogicalJoinOperator newJoinOp = buildNewJoinOperator(join);
            OptExpressionWithOutput toOpt = buildJoinOptExpression(context, joinOutputColRefs,
                    newJoinOp, tvrLeftTo.optExpression(), tvrRightTo.optExpression(), false);
            return new TvrOptExpression(tvrLeftTo.tvrVersionRange(), toOpt.optExpression());
        });
        // root opt group
        return new TvrOptMeta(joinDeltaTrait, fromJoin, toJoin);
    }

    private List<OptExpressionWithOutput> buildCommonJoinDelta(OptimizerContext context,
                                                               LogicalJoinOperator join,
                                                               List<ColumnRefOperator> joinOutputColRefs,
                                                               TvrOptExpression tvrLeftFrom,
                                                               OptExpression rightDelta,
                                                               TvrOptExpression tvrRightTo,
                                                               OptExpression leftDelta) {
        OptExpressionWithOutput deltaOutput1 =
                buildJoinOptExpression(context, joinOutputColRefs, join, tvrLeftFrom.optExpression(),
                        rightDelta, true);
        OptExpressionWithOutput deltaOutput2 =
                buildJoinOptExpression(context, joinOutputColRefs, join, leftDelta,
                        tvrRightTo.optExpression(), true);
        return Lists.newArrayList(deltaOutput1, deltaOutput2);
    }

    private OptExpression doTransformWithMonotonic(OptimizerContext context,
                                                   OptExpression input,
                                                   LogicalJoinOperator join,
                                                   List<ColumnRefOperator> joinOutputColRefs,
                                                   TvrOptMeta leftOptMeta,
                                                   TvrOptMeta rightOptMeta) {
        TvrOptExpression tvrLeftFrom = leftOptMeta.getFrom();
        TvrOptExpression tvrLeftTo = leftOptMeta.getTo();
        TvrOptExpression tvrRightFrom = rightOptMeta.getFrom();
        TvrOptExpression tvrRightTo = rightOptMeta.getTo();
        OptExpression leftDelta = input.inputAt(0);
        OptExpression rightDelta = input.inputAt(1);

        JoinOperator joinType = join.getJoinType();
        if (joinType.isInnerJoin() || joinType.isCrossJoin()) {
            // build the tvrOptMeta for the join operator, use the left table's tvrOptMeta as the root
            List<OptExpressionWithOutput> commonJoinDelta =
                    buildCommonJoinDelta(context, join, joinOutputColRefs, tvrLeftFrom, rightDelta, tvrRightTo, leftDelta);
            // build the join tvrOptMeta
            TvrOptMeta joinTvrOptMeta = buildJoinTvrOptMeta(context, join, leftOptMeta, rightOptMeta, joinOutputColRefs,
                    leftOptMeta.tvrDeltaTrait());
            // return a union operator to merge the common join delta
            return buildUnionOperator(joinTvrOptMeta, joinOutputColRefs, commonJoinDelta);
        } else {
            throw new IllegalStateException(
                    "Unsupported join type for TVR: " + join.getJoinType() + " in " + input);
        }
    }
}
