// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

public class AggregateChecker {
    private List<ScalarOperator> mvAggregates;
    private boolean distinct;

    public AggregateChecker(List<ScalarOperator> mvAggregates) {
        this.mvAggregates = mvAggregates;
        this.distinct = false;
    }

    // true if all matched, or false
    public boolean check(List<ScalarOperator> aggregates) {
        AggregateCheckVisitor visitor = new AggregateCheckVisitor();
        for (ScalarOperator agg : aggregates) {
            boolean matched = agg.accept(visitor, null);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    public boolean hasDistinct() {
        return distinct;
    }

    private class AggregateCheckVisitor extends ScalarOperatorVisitor<Boolean, Void> {
        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            // Aggregate must be CallOperator
            return isMatched(scalarOperator);
        }

        @Override
        public Boolean visitCall(CallOperator callOperator, Void context) {
            // Aggregate must be CallOperator
            if (callOperator.isDistinct()) {
                distinct = true;
            }
            return isMatched(callOperator);
        }

        boolean isMatched(ScalarOperator scalarOperator) {
            // judge child first
            boolean childMatched = true;
            for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
                if (scalarOperator.getChild(i).isVariable()) {
                    Boolean matched = scalarOperator.getChild(i).accept(this, null);
                    if (!Boolean.TRUE.equals(matched)) {
                        childMatched = false;
                    }
                }
            }
            if (mvAggregates.contains(scalarOperator)) {
                return true;
            }
            return childMatched;
        }
    }
}
