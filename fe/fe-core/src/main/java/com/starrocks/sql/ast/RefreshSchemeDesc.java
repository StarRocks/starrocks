// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.RefreshType;

public class RefreshSchemeDesc implements ParseNode {

    protected RefreshType type;

    private final ImmutableSet<TimestampArithmeticExpr.TimeUnit> supportedTimeUnitType = ImmutableSet.of(
            TimestampArithmeticExpr.TimeUnit.MINUTE,
            TimestampArithmeticExpr.TimeUnit.HOUR,
            TimestampArithmeticExpr.TimeUnit.DAY,
            TimestampArithmeticExpr.TimeUnit.WEEK,
            TimestampArithmeticExpr.TimeUnit.MONTH,
            TimestampArithmeticExpr.TimeUnit.YEAR
    );
    public RefreshSchemeDesc(RefreshType type) {
        this.type = type;
    }

    public RefreshType getType() {
        return type;
    }

}
