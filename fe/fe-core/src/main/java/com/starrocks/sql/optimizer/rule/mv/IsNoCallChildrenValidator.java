// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

// If the children of scalarOperator are all ConstantOperator or ColumnRefOperator, will return true.
// Currently, mainly support CaseWhenOperator.
public class IsNoCallChildrenValidator extends ScalarOperatorVisitor<Boolean, Void> {
    @Override
    public Boolean visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        // now support simple type: all of element is constant or column ref operator
        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            ScalarOperator whenClause = operator.getWhenClause(i);
            if (!whenClause.accept(this, null)) {
                return false;
            }
            ScalarOperator thenClause = operator.getThenClause(i);
            if (!thenClause.accept(this, null)) {
                return false;
            }
            if (operator.hasElse()) {
                ScalarOperator elseClause = operator.getElseClause();
                if (!elseClause.accept(this, null)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        List<ScalarOperator> children = predicate.getChildren();
        for (ScalarOperator childOp : children) {
            if (!childOp.accept(this, null)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitConstant(ConstantOperator literal, Void context) {
        return true;
    }

    @Override
    public Boolean visitVariableReference(ColumnRefOperator variable, Void context) {
        return true;
    }

    @Override
    public Boolean visit(ScalarOperator scalarOperator, Void context) {
        return false;
    }
}
