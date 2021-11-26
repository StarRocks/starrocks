// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.RangeExtractor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RemoveDuplicatePredicateRule extends BottomUpScalarOperatorRewriteRule {
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        RangeExtractor rangeExtractor = new RangeExtractor();
        Map<ScalarOperator, RangeExtractor.ValueDescriptor> hashMap = rangeExtractor.apply(predicate, null);

        List<ScalarOperator> predicates = new ArrayList<>();
        for (RangeExtractor.ValueDescriptor valueDescriptor : hashMap.values()) {
            predicates.addAll(valueDescriptor.toScalarOperator());

            if (valueDescriptor.getSourceCount() <= 1) {
                return predicate;
            }
        }
        if (predicates.isEmpty()) {
            return predicate;
        }

        ScalarOperator p = Utils.compoundAnd(predicates);

        if (isOr(predicate) && hashMap.size() == 1 && !p.equals(predicate) &&
                hashMap.values().stream().allMatch(v -> v instanceof RangeExtractor.MultiValuesDescriptor)) {
            return p;
        }

        if (isAnd(predicate) && hashMap.size() == 1 && !p.equals(predicate) &&
                hashMap.values().stream().allMatch(v -> v instanceof RangeExtractor.RangeDescriptor)) {
            return p;
        }

        return predicate;
    }

    private boolean isAnd(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (compoundPredicateOperator.isOr() || compoundPredicateOperator.isNot()) {
                return false;
            }

            return isAnd(compoundPredicateOperator.getChild(0)) && isAnd(compoundPredicateOperator.getChild(1));
        } else {
            return true;
        }
    }

    private boolean isOr(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (compoundPredicateOperator.isAnd() || compoundPredicateOperator.isNot()) {
                return false;
            }

            return isOr(predicate.getChild(0)) && isOr(predicate.getChild(1));
        } else {
            return true;
        }
    }
}
