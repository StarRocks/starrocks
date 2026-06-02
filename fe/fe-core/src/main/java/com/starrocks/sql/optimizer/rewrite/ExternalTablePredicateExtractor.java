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


package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.JDBCTable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.LinkedList;
import java.util.List;

// Extract predicates that can be pushed down to external table
// and predicates that must be reserved
// from the entire predicate
// To be safe, we only allow push down simple  predicates
public class ExternalTablePredicateExtractor {

    private final JDBCTable.ProtocolType dialect;
    private List<ScalarOperator> pushedPredicates = new LinkedList<>();
    private List<ScalarOperator> reservedPredicates = new LinkedList<>();

    public ExternalTablePredicateExtractor(JDBCTable.ProtocolType dialect) {
        this.dialect = dialect;
    }

    public ScalarOperator getPushPredicate() {
        return Utils.compoundAnd(pushedPredicates);
    }

    public ScalarOperator getReservePredicate() {
        return Utils.compoundAnd(reservedPredicates);
    }

    public void extract(ScalarOperator op) {
        if (op.getOpType().equals(OperatorType.COMPOUND)) {
            CompoundPredicateOperator operator = (CompoundPredicateOperator) op;
            switch (operator.getCompoundType()) {
                case AND: {
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(operator);
                    // for CNF, we can push down each predicate independently
                    for (ScalarOperator conjunct : conjuncts) {
                        if (CanPushDownPredicateVisitor.canPushDown(conjunct, dialect)) {
                            pushedPredicates.add(removeImplicitCast(conjunct));
                        } else {
                            reservedPredicates.add(conjunct);
                        }
                    }
                    return;
                }
                case OR: {
                    // for DNF, pushdown is only possible if all children can be pushed down
                    for (ScalarOperator child : operator.getChildren()) {
                        if (!CanPushDownPredicateVisitor.canPushDown(child, dialect)) {
                            reservedPredicates.add(op);
                            return;
                        }
                    }
                    pushedPredicates.add(removeImplicitCast(operator));
                    return;
                }
                case NOT: {
                    if (CanPushDownPredicateVisitor.canPushDown(op.getChild(0), dialect)) {
                        pushedPredicates.add(removeImplicitCast(op));
                    } else {
                        reservedPredicates.add(op);
                    }

                    return;
                }
            }
            return;
        }
        if (CanPushDownPredicateVisitor.canPushDown(op, dialect)) {

            pushedPredicates.add(removeImplicitCast(op));
        } else {
            reservedPredicates.add(op);
        }
    }

    private ScalarOperator removeImplicitCast(ScalarOperator operator) {
        BaseScalarOperatorShuttle removeImplicitCastShuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
                boolean[] update = {false};
                List<ScalarOperator> clonedOperators = visitList(operator.getChildren(), update);
                if (operator.isImplicit()) {
                    return update[0] ? clonedOperators.get(0) : operator.getChild(0);
                } else {
                    return update[0] ? new CastOperator(operator.getType(), clonedOperators.get(0), operator.isImplicit())
                            : operator;
                }
            }
        };

        return operator.accept(removeImplicitCastShuttle, null);
    }
}
