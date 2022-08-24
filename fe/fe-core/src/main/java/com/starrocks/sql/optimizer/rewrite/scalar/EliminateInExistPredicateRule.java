// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.Subquery;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;

import java.util.List;

public class EliminateInExistPredicateRule extends BaseScalarOperatorRewriteRule {

    private final Expr predicate;
    private final ExpressionMapping expressionMapping;

    public EliminateInExistPredicateRule(Expr predicate,
                                         ExpressionMapping expressionMapping) {
        this.predicate = predicate;
        this.expressionMapping = expressionMapping;
    }

    @Override
    public boolean isOneShot() {
        return true;
    }

    @Override
    public ScalarOperator visit(ScalarOperator operator, ScalarOperatorRewriteContext context) {
        List<InPredicate> inPredicates = Lists.newArrayList();
        predicate.collect(InPredicate.class, inPredicates);

        List<ExistsPredicate> existsSubqueries = Lists.newArrayList();
        predicate.collect(ExistsPredicate.class, existsSubqueries);

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(operator);

        for (InPredicate e : inPredicates) {
            if (!(e.getChild(1) instanceof Subquery)) {
                continue;
            }

            if (((Subquery) e.getChild(1)).isUseSemiAnti()) {
                ColumnRefOperator columnRefOperator = expressionMapping.get(e);
                conjuncts.remove(columnRefOperator);
            }
        }

        for (ExistsPredicate e : existsSubqueries) {
            Preconditions.checkState(e.getChild(0) instanceof Subquery);
            if (((Subquery) e.getChild(0)).isUseSemiAnti()) {
                ColumnRefOperator columnRefOperator = expressionMapping.get(e);
                conjuncts.remove(columnRefOperator);
            }
        }
        return Utils.compoundAnd(conjuncts);
    }
}
