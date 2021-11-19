// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

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

        OptExpression repeatOpt = input.getInputs().get(0);
        LogicalRepeatOperator logicalRepeatOperator = (LogicalRepeatOperator) repeatOpt.getOp();
        Set<ColumnRefOperator> repeatColumns = Sets.newHashSet();
        logicalRepeatOperator.getRepeatColumnRef().forEach(repeatColumns::addAll);

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();

        for (ScalarOperator predicate : predicates) {
            // push down predicate
            if (canPushDownPredicate(predicate.clone(), repeatColumns)) {
                pushDownPredicates.add(predicate);
            }
        }

        // merge filter
        predicates.add(logicalRepeatOperator.getPredicate());
        logicalRepeatOperator.setPredicate(Utils.compoundAnd(predicates));

        // push down
        if (pushDownPredicates.size() > 0) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
            OptExpression oe = new OptExpression(newFilter);
            oe.getInputs().addAll(repeatOpt.getInputs());

            repeatOpt.getInputs().clear();
            repeatOpt.getInputs().add(oe);
        }

        return Lists.newArrayList(repeatOpt);
    }

    /**
     * 1: replace the column of nullColumns in  expression to NULL literal
     * 2: Call the ScalarOperatorRewriter function to perform constant folding
     * 3: If the result of constant folding is true or can't reduction,
     *    it proves that the expression may contains null value, can not push down
     */
    private boolean canPushDownPredicate(ScalarOperator predicate, Set<ColumnRefOperator> repeatColumns) {
        Map<ColumnRefOperator, ScalarOperator> m =
                repeatColumns.stream().map(col -> new ColumnRefOperator(col.getId(), Type.INVALID, "", true))
                        .collect(Collectors.toMap(identity(), col -> ConstantOperator.createNull(col.getType())));

        ScalarOperator nullEval = new ReplaceColumnRefRewriter(m).visit(predicate, null);

        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        // The calculation of the null value is in the constant fold
        nullEval = scalarRewriter.rewrite(nullEval, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        if (nullEval.equals(ConstantOperator.createBoolean(true))) {
            return false;
        } else if (!(nullEval instanceof ConstantOperator)) {
            return false;
        }
        return true;
    }
}
