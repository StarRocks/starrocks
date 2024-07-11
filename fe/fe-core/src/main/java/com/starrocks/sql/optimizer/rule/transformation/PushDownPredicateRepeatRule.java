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
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.groovy.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PushDownPredicateRepeatRule extends TransformationRule {
    public PushDownPredicateRepeatRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_REPEAT,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_REPEAT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator logicalFilterOperator = (LogicalFilterOperator) input.getOp();
        List<ScalarOperator> predicates = Utils.extractConjuncts(logicalFilterOperator.getPredicate());

        LogicalRepeatOperator logicalRepeatOperator = (LogicalRepeatOperator) input.inputAt(0).getOp();
        Set<ColumnRefOperator> repeatColumns = Sets.newHashSet();
        logicalRepeatOperator.getRepeatColumnRef().forEach(repeatColumns::addAll);

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();

        for (ScalarOperator predicate : predicates) {
            // push down predicate
            if (canPushDownPredicate(predicate, repeatColumns)) {
                pushDownPredicates.add(predicate);
            }
        }

        // merge filter
        predicates.add(logicalRepeatOperator.getPredicate());
        OptExpression opt = OptExpression.create(new LogicalRepeatOperator.Builder().withOperator(logicalRepeatOperator)
                        .setPredicate(Utils.compoundAnd(predicates))
                        .build(),
                input.inputAt(0).getInputs());

        // push down
        if (pushDownPredicates.size() > 0) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
            OptExpression oe = new OptExpression(newFilter);
            oe.getInputs().addAll(opt.getInputs());
            opt.setChild(0, oe);
        }

        return Lists.newArrayList(opt);
    }

    /**
     * 1: replace the column of nullColumns in  expression to NULL literal
     * 2: Call the ScalarOperatorRewriter function to perform constant folding
     * 3: If the result of constant folding is true or can't reduction,
     * it proves that the expression may contains null value, can not push down
     */
    private boolean canPushDownPredicate(ScalarOperator predicate, Set<ColumnRefOperator> repeatColumns) {
        ColumnRefSet usedRefs = predicate.getUsedColumns();
        if (!usedRefs.containsAny(repeatColumns)) {
            return false;
        }

        for (ColumnRefOperator repeatColumn : repeatColumns) {
            if (!usedRefs.contains(repeatColumn)) {
                continue;
            }
            // need check repeat column one by one
            Map<ColumnRefOperator, ScalarOperator> m =
                    Maps.of(repeatColumn, ConstantOperator.createNull(repeatColumn.getType()));
            ScalarOperator nullEval = new ReplaceColumnRefRewriter(m).rewrite(predicate);

            ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
            // The calculation of the null value is in the constant fold
            nullEval = scalarRewriter.rewrite(nullEval, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if (nullEval.equals(ConstantOperator.createBoolean(true))) {
                return false;
            } else if (!(nullEval instanceof ConstantOperator)) {
                return false;
            }
        }
        return true;
    }
}
