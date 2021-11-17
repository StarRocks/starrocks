// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalCTEAnchorOperator extends PhysicalOperator {
    private String cteId;

    public PhysicalCTEAnchorOperator(String cteId) {
        super(OperatorType.PHYSICAL_CTE_ANCHOR);
        this.cteId = cteId;
    }
}
