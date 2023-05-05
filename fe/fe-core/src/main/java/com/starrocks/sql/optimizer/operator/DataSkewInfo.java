// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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