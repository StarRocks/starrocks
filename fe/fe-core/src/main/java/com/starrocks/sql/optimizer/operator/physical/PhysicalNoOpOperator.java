// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalNoOpOperator extends PhysicalOperator {
    public PhysicalNoOpOperator() {
        super(OperatorType.PHYSICAL_NO_OP);
    }
}
