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

package com.starrocks.sql.automv.estimation;

import com.starrocks.sql.automv.lattice.LatticeNodeId;

import java.util.Objects;

public final class CardRecord {
    private final LatticeNodeId id;
    private long rowCount;
    private long cardinality;
    private boolean convergent;
    private double cardRowCountRatio;

    public CardRecord(LatticeNodeId id) {
        this.id = Objects.requireNonNull(id);
        this.rowCount = 0;
        this.cardinality = 0;
        this.convergent = false;
        this.cardRowCountRatio = 1.0;
    }

    public LatticeNodeId getId() {
        return id;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getCardinality() {
        return cardinality;
    }

    // A MV's Benefit means that how much benefit we can get from using MV
    // acceleration instead of executing original query. At present, the
    // benefit is calculated by subtracting as the number of rows of the flat table from
    // the the number of rows of the MV.
    // 1. rowCount means the number of flat table;
    // 2. cardinality means cardinality of MV's dimensions, i.e. the number of rows of MV.
    //
    // TODO(by satanson): The benefit calculation is from standard Lattice-based algorithm,
    //  at present, only the the number of rows reduced when using MV acceleration is taken
    //  into consideration, in the future, other factors such as the cost differences of
    //  aggregation function should be considered too.
    public double getBenefit() {
        return (double) (rowCount - cardinality);
    }

    public boolean isConvergent() {
        return convergent;
    }

    public double getCardRowCountRatio() {
        return cardRowCountRatio;
    }

    public void updateCard(long rowCount, long card, long minSamplingRows, double relativeErrorThreshold) {
        card = Math.min(rowCount, card);
        this.rowCount = rowCount;
        this.cardinality = card;
        double prevCardRowCountRatio = this.cardRowCountRatio;
        double currCardRowCountRatio = (double) this.cardinality / Math.max(1, rowCount);

        double absError = Math.abs(prevCardRowCountRatio - currCardRowCountRatio);
        double maxCardRowCountRatio = Math.max(prevCardRowCountRatio, currCardRowCountRatio);
        double relError = maxCardRowCountRatio == 0.0 ? Double.MAX_VALUE : absError / maxCardRowCountRatio;
        relError = Double.isFinite(relError) ? relError : Double.MAX_VALUE;
        this.convergent = this.rowCount > minSamplingRows && relError < relativeErrorThreshold;
        this.cardRowCountRatio = currCardRowCountRatio;
    }
}
