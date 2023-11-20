package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public interface PredicateReplaceChecker {
    boolean canReplace(ScalarOperator operator);
}
