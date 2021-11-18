// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalCTEProduceOperator extends PhysicalOperator {
    private String cteId;

    public PhysicalCTEProduceOperator(String cteId) {
        super(OperatorType.PHYSICAL_CTE_PRODUCE);
        this.cteId = cteId;
    }
}
