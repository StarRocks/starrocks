// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.BitSet;
import java.util.List;

// TODO: now depends on equal
// This will be extended to rollup(like date_trunc)
public class GroupKeyChecker {
    private final List<ScalarOperator> mvGroupKeys;
    private final BitSet matchingBits;

    public GroupKeyChecker(List<ScalarOperator> mvGroupKeys) {
        this.mvGroupKeys = mvGroupKeys;
        this.matchingBits = new BitSet(mvGroupKeys.size());
    }

    public boolean isRollup() {
        return matchingBits.cardinality() < mvGroupKeys.size();
    }

    // true if matched, or false
    public boolean check(List<ScalarOperator> groupKeys) {
        GroupKeyCheckVisitor visitor = new GroupKeyCheckVisitor();
        for (ScalarOperator key : groupKeys) {
            boolean matched = key.accept(visitor, null);
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private class GroupKeyCheckVisitor extends ScalarOperatorVisitor<Boolean, Object> {
        @Override
        public Boolean visit(ScalarOperator scalarOperator, Object context) {
            for (int i = 0; i < mvGroupKeys.size(); i++) {
                if (mvGroupKeys.get(i).equals(scalarOperator)) {
                    matchingBits.set(i);
                    return true;
                }
            }
            return false;
        }
    }
}
