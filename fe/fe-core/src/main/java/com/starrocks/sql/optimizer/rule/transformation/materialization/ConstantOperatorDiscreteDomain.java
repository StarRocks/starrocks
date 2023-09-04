package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.DiscreteDomain;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.jetbrains.annotations.Nullable;

public class ConstantOperatorDiscreteDomain extends DiscreteDomain<ConstantOperator> {
    @Nullable
    @Override
    public ConstantOperator next(ConstantOperator value) {
        return value.successor().get();
    }

    @Nullable
    @Override
    public ConstantOperator previous(ConstantOperator value) {
        return value.predecessor().get();
    }

    @Override
    public long distance(ConstantOperator start, ConstantOperator end) {
        return start.distance(end);
    }
}
