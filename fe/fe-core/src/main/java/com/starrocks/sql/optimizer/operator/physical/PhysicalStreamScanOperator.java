// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalStreamScanOperator extends PhysicalOperator {

    public PhysicalStreamScanOperator(OperatorType type) {
        super(type);
    }
}
