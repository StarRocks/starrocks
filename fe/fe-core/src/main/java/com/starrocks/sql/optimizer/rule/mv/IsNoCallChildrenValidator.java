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
            // SUM(NULL) always NULL,
            // SUM(1) on base table result is 10, but in mv maybe is 5 because mv will aggregate by key
            return ((ConstantOperator) operator.getChild(0)).isNull();
        }

        // Range of decimal/double/float is greater than most number type, so there will ignore cast to decimal/double,
        // but it's not accurate, such cast(double to float)
        if (operator.getType().isDecimalOfAnyVersion() || operator.getType().isFloatingPointType()) {
            return operator.getChild(0).accept(this, context);
        }

        // cast big type to small type, forbidden
        if (operator.getType().getTypeSize() >= operator.getChild(0).getType().getTypeSize()) {
            return operator.getChild(0).accept(this, context);
        }

        return false;
    }

    @Override
    public Boolean visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        if (operator.hasCase() &&
                !Utils.extractColumnRef(operator.getCaseClause()).stream().allMatch(keyColumns::contains)) {
            return false;
        }

        if (operator.hasElse()) {
            ScalarOperator elseClause = operator.getElseClause();
            // e.g. select v1, SUM(case v2 when 2 then v3 when 4 then v4 else v5 end) from xxx;
            // return result must be column or NULL, const value will cause error result.
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
            if (!Utils.extractColumnRef(thenClause).stream().allMatch(aggregateColumns::contains) ||
                    (thenClause.isConstantRef() && !((ConstantOperator) thenClause).isNull()) ||
                    thenClause.isConstant()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Boolean visitCall(CallOperator operator, Void context) {
        if (FunctionSet.IF.equalsIgnoreCase(operator.getFnName())) {
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
        return false;
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
