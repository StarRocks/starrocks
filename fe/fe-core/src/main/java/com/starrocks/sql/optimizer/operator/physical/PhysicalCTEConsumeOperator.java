// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalCTEConsumeOperator extends PhysicalOperator {
    private final String cteId;

    public PhysicalCTEConsumeOperator(String cteId) {
        super(OperatorType.PHYSICAL_CTE_CONSUME);
        this.cteId = cteId;
    }

    public String getCteId() {
        return cteId;
    }
}
