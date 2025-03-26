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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;

import java.util.List;

public class InnerToSemiRule extends TransformationRule {

    public InnerToSemiRule() {
        super(RuleType.TF_INNER_TO_SEMI, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_MULTIJOIN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator logicalAggregationOperator = (LogicalAggregationOperator) input.getOp();
        // support distinct on multi join node
        if (logicalAggregationOperator.getAggregations() != null &&
                !logicalAggregationOperator.getAggregations().isEmpty()) {
            return Lists.newArrayList(input);
        }

        ColumnRefSet columnRefSet = input.getChildOutputColumns(0);
        ColumnRefSet back = context.getTaskContext().getRequiredColumns();
        context.getTaskContext().setRequiredColumns(columnRefSet);

        OptExpression newChild = new ReorderJoinRule().rewrite_for_distinct_join(input.inputAt(0), context);
        context.getTaskContext().setRequiredColumns(back);

        newChild = rewriteInnerToSemi(newChild, true);
        input.setChild(0, newChild);

        return Lists.newArrayList(input);
    }

    private OptExpression rewriteInnerToSemi(OptExpression opt, boolean parentIsInnerOrDistinct) {
        if (opt.getOp() instanceof LogicalJoinOperator joinOperator) {
            if (!joinOperator.getJoinHint().isEmpty() || joinOperator.getPredicate() != null) {
                return opt;
            }

            if (joinOperator.getJoinType() == JoinOperator.INNER_JOIN) {
                if (parentIsInnerOrDistinct) {
                    List<OptExpression> children = opt.getInputs();
                    for (int i = 0; i < children.size(); i++) {
                        OptExpression child = children.get(i);
                        if (!opt.getOutputColumns().isIntersect(child.getOutputColumns())) {
                            if (i == 0) {
                                joinOperator = new LogicalJoinOperator(JoinOperator.RIGHT_SEMI_JOIN,
                                        joinOperator.getOnPredicate());
                            } else {
                                joinOperator = new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN,
                                        joinOperator.getOnPredicate());
                            }

                            opt = OptExpression.create(joinOperator, opt.getInputs());
                            break;
                        }
                    }
                }
                for (int i = 0; i < opt.getInputs().size(); i++) {
                    opt.getInputs().set(i, rewriteInnerToSemi(opt.getInputs().get(i), true));
                }
            }
        }

        return opt;
    }
}
