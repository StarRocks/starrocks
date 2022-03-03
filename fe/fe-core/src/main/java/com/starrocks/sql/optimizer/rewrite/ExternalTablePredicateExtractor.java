// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

// Extract predicates that can be pushed down to external table
// and predicates that must be reserved
// from the entire predicate
// To be safe, we only allow push down simple binary predicates and some simple functions in functionWhiteList
public class ExternalTablePredicateExtractor {
    private static final Set<String> functionWhiteList = new HashSet<>();
    static {
        functionWhiteList.addAll(ImmutableList.of(
                "cast"
        ));
    }

    private List<ScalarOperator> pushedPredicates = new LinkedList<>();
    private List<ScalarOperator> reservedPredicates = new LinkedList<>();

    public ScalarOperator getPushPredicate() {
        return Utils.compoundAnd(pushedPredicates);
    }

    public ScalarOperator getReservePredicate() {
        return Utils.compoundAnd(reservedPredicates);
    }

    public void extract(ScalarOperator op) {
        pushedPredicates.clear();
        reservedPredicates.clear();
        if (op.getOpType().equals(OperatorType.COMPOUND)) {
            CompoundPredicateOperator operator = (CompoundPredicateOperator) op;
            switch (operator.getCompoundType()) {
                case AND: {
                    // for CNF, we can push down each predicate independently
                    for (ScalarOperator child : operator.getChildren()) {
                        if (canPushdown(child)) {
                            pushedPredicates.add(child);
                        } else {
                            reservedPredicates.add(child);
                        }
                    }
                    return;
                }
                case OR: {
                    // for DNF, pushdown is only possible if all children can be pushed down
                    for (ScalarOperator child : operator.getChildren()) {
                        if (!canPushdown(child)) {
                            reservedPredicates.add(op);
                            return;
                        }
                    }
                    pushedPredicates.add(operator);
                    return;
                }
                case NOT: {
                    extract(op.getChild(0));
                    return;
                }
            }
            return;
        }
        if (canPushdown(op)) {
            pushedPredicates.add(op);
        } else {
            reservedPredicates.add(op);
        }
    }

    // check whether a predicate can be pushed down as a whole
    public boolean canPushdown(ScalarOperator op) {
        if (op == null) {
            return false;
        }
        switch (op.getOpType()) {
            case IS_NULL:
            case BINARY:
            case COMPOUND: {
                for (ScalarOperator child : op.getChildren()) {
                    if (!canPushdown(child)) {
                        return false;
                    }
                }
                return true;
            }
            case VARIABLE:
            case CONSTANT: {
                return true;
            }
            case CALL: {
                CallOperator callOperator = (CallOperator) op;
                String fnName = callOperator.getFnName();
                if (functionWhiteList.contains(fnName)) {
                    for (ScalarOperator child : op.getChildren()) {
                        if (!canPushdown(child)) {
                            return false;
                        }
                    }
                    return true;
                }
                break;
            }
            default:
                break;
        }
        return false;
    }
}
