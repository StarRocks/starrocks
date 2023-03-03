// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

public class DataSkewInfo {
    private ColumnRefOperator skewColumnRef;
    private double penaltyFactor = 1.0;
    private int stage = 0;

    public ColumnRefOperator getSkewColumnRef() {
        return skewColumnRef;
    }

    public void setSkewColumnRef(ColumnRefOperator skewColumnRef) {
        this.skewColumnRef = skewColumnRef;
    }

    public double getPenaltyFactor() {
        return penaltyFactor;
    }

    public void setPenaltyFactor(double penaltyFactor) {
        this.penaltyFactor = penaltyFactor;
    }

    public int getStage() {
        return stage;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }

    public DataSkewInfo(ColumnRefOperator skewColumnRef, double penaltyFactor, int stage) {
        this.skewColumnRef = skewColumnRef;
        this.penaltyFactor = penaltyFactor;
        this.stage = stage;
    }
}