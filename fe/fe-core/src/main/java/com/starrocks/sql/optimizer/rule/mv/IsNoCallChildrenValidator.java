// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

// If the children of scalarOperator are all ConstantOperator or ColumnRefOperator, will return true.
// Currently, mainly support CaseWhenOperator.
public class IsNoCallChildrenValidator extends ScalarOperatorVisitor<Boolean, Void> {
    private final ColumnRefSet keyColumns;

    private final ColumnRefSet aggregateColumns;

    public IsNoCallChildrenValidator(ColumnRefSet keyColumns, ColumnRefSet aggregateColumns) {
        this.keyColumns = keyColumns;
        this.aggregateColumns = aggregateColumns;
    }

    @Override
    public Boolean visitCastOperator(CastOperator operator, Void context) {
        if (operator.getChild(0).isColumnRef()) {
            return aggregateColumns.contains((ColumnRefOperator) operator.getChild(0));
        }

        if (operator.getChild(0).isConstantRef()) {
            return ((ConstantOperator) operator.getChild(0)).isNull();
        }

        if (operator.getType().isDecimalOfAnyVersion()) {
            return operator.getChild(0).accept(this, context);
        }

        // cast big type to small type, forbidden
        if (operator.getType().getSlotSize() >= operator.getChild(0).getType().getSlotSize()) {
            return operator.getChild(0).accept(this, context);
        }

        return false;
    }

    @Override
    public Boolean visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        // now support simple type: all of element is constant or column ref operator
        if (operator.hasCase() &&
                !Utils.extractColumnRef(operator.getCaseClause()).stream().allMatch(keyColumns::contains)) {
            return false;
        }

        if (operator.hasElse()) {
            ScalarOperator elseClause = operator.getElseClause();
            if ((elseClause.isColumnRef() && aggregateColumns.contains((ColumnRefOperator) elseClause)) ||
                    (elseClause.isConstantRef() && ((ConstantOperator) elseClause).isNull())) {
                // pass
            } else {
                return false;
            }
        }

        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            ScalarOperator whenClause = operator.getWhenClause(i);
            if (!Utils.extractColumnRef(whenClause).stream().allMatch(keyColumns::contains)) {
                return false;
            }
            ScalarOperator thenClause = operator.getThenClause(i);
            if (!Utils.extractColumnRef(thenClause).stream().allMatch(aggregateColumns::contains)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Boolean visitCall(CallOperator operator, Void context) {
        if (!FunctionSet.IF.equalsIgnoreCase(operator.getFnName())) {
            return false;
        }

        if (!Utils.extractColumnRef(operator.getChild(0)).stream().allMatch(keyColumns::contains)) {
            return false;
        }

        if (!Utils.extractColumnRef(operator.getChild(1)).stream().allMatch(aggregateColumns::contains)) {
            return false;
        }

        if (!Utils.extractColumnRef(operator.getChild(2)).stream().allMatch(aggregateColumns::contains)) {
            return false;
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
    public Boolean visit(ScalarOperator scalarOperator, Void context) {
        return false;
    }
}
