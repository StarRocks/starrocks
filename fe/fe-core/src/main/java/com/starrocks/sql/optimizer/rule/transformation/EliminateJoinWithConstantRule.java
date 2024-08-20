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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

/**
 * Eliminate join with constant values which is a single row.
 * eg:
 * input: select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col from lineitem_partition t1 join (select '2000-01-01' as col) t2 on true
 * output: select t1.L_ORDERKEY, t1.L_PARTKEY, '2000-01-01' as col from lineitem_partition t1
 */
public class EliminateJoinWithConstantRule extends TransformationRule {
    public static final EliminateJoinWithConstantRule ELIMINATE_JOIN_WITH_LEFT_SINGLE_VALUE_RULE =
            new EliminateJoinWithConstantRule(0);
    public static final EliminateJoinWithConstantRule ELIMINATE_JOIN_WITH_RIGHT_SINGLE_VALUE_RULE =
            new EliminateJoinWithConstantRule(1);

    private final int constantIndex;
    private EliminateJoinWithConstantRule(int index) {
        super(RuleType.TF_ELIMINATE_JOIN_WITH_CONSTANT, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
        this.constantIndex = index;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (OperatorType.LOGICAL_PROJECT.equals(input.inputAt(constantIndex).getOp().getOpType())) {
            OptExpression optExpression = input.inputAt(constantIndex);
            OptExpression valuesOpt = optExpression.inputAt(0);
            return checkValuesOptExpression(valuesOpt);
        }
        return false;
    }

    public boolean checkValuesOptExpression(OptExpression valuesOpt) {
        if (valuesOpt.getOp().getOpType() != OperatorType.LOGICAL_VALUES) {
            return false;
        }
        LogicalValuesOperator v = valuesOpt.getOp().cast();
        // only prune joins with one row
        return v.getRows().size() == 1;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (!isTransformable((LogicalJoinOperator) input.getOp(), constantIndex)) {
            return Lists.newArrayList(input);
        }
        OptExpression otherOpt = input.inputAt(1 - constantIndex);
        OptExpression valueOpt = input.inputAt(constantIndex);
        return onMatch(input, otherOpt, valueOpt, context);
    }

    private boolean isTransformable(LogicalJoinOperator joinOperator,
                                    int constantIndex) {
        JoinOperator joinType = joinOperator.getJoinType();
        // anti/full outer join cannot be eliminated.
        // semi join needs to distinct output which cannot be eliminated.
        if (joinType.isAntiJoin() || joinType.isFullOuterJoin() || joinType.isSemiJoin()) {
            return false;
        }
        if (constantIndex == 0) {
            // constant values is left, so we can't eliminate left outer join
            return !joinOperator.getJoinType().isLeftOuterJoin();
        } else {
            // constant values is right, so we can't eliminate right outer join
            return !joinOperator.getJoinType().isRightOuterJoin();
        }
    }

    public List<OptExpression> onMatch(OptExpression joinOpt,
                                       OptExpression otherOpt,
                                       OptExpression constantOpt,
                                       OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> outputs = Maps.newHashMap();
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOpt.getOp();
        JoinOperator joinType = joinOperator.getJoinType();
        ScalarOperator condition = joinOperator.getOnPredicate();
        ScalarOperator predicate = otherOpt.getOp().getPredicate();
        // rewrite join's on-predicate with constant column values
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) constantOpt.getOp();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projectOperator.getColumnRefMap());
        ScalarOperator rewrittenCondition = rewriter.rewrite(condition);
        // output join and constant opt's output columns
        joinOpt.getOutputColumns().getStream().map(context.getColumnRefFactory()::getColumnRef)
                .forEach(ref -> outputs.put(ref, rewriter.rewrite(ref)));
        if (joinOperator.getJoinType().isOuterJoin()) {
            // transform join's on-predicate with case-when operator
            constantOpt.getRowOutputInfo().getColumnRefMap().entrySet().stream()
                    .forEach(entry -> {
                        ScalarOperator transformed = transformOuterJoinOnPredicate(
                                joinOperator, entry.getValue(), rewrittenCondition);
                        outputs.put(entry.getKey(), transformed);
                    });
        } else {
            predicate = Utils.compoundAnd(predicate, rewrittenCondition);
        }
        LogicalProjectOperator project = new LogicalProjectOperator(outputs);
        OptExpression result = OptExpression.create(project, otherOpt);

        // save predicate
        if (predicate != null) {
            result = OptExpression.create(new LogicalFilterOperator(predicate), result);
        }
        return Lists.newArrayList(result);
    }

    /**
     * Transform on-predicate for outer join, add null value for the columns of join operator.
     * eg:
     * input:
     *  select t1.L_ORDERKEY, t1.L_PARTKEY, t2.col
     *      from lineitem_partition t1 left outer join (select '2000-01-01' as col) t2 on t1.L_SHIPDATE = t2.col
     *
     * output:
     *  select t1.L_ORDERKEY, t1.L_PARTKEY, case when t1.L_SHIPDATE = '2000-01-01' then '2000-01-01' else null end as col
     * @param joinOperator input join operator
     * @param value constant value
     * @param condition join's on-predicate
     * @return transformed scalar operator which adds null value for the columns of join operator
     */
    private ScalarOperator transformOuterJoinOnPredicate(LogicalJoinOperator joinOperator,
                                                         ScalarOperator value,
                                                         ScalarOperator condition) {
        if (!joinOperator.getJoinType().isOuterJoin() || condition == null || condition.isConstantTrue()) {
            return value;
        }
        // if the join type is outer join, we need to add null value for the columns of join operator
        // eg: case when condition then value else null
        List<ScalarOperator> whenThen = Lists.newArrayList();
        whenThen.add(condition);
        whenThen.add(value);
        return new CaseWhenOperator(value.getType(), null, ConstantOperator.createNull(value.getType()), whenThen);
    }
}
