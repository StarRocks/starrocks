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

package com.starrocks.sql.automv.lattice;

import java.util.Objects;

public final class TentativeQueryBenefit {
    private final LatticeNodeId id;
    private final int index;

    private boolean selected;
    private double cost = Double.MAX_VALUE;
    private double benefit = Double.MIN_VALUE;

    public TentativeQueryBenefit(LatticeNodeId id, int index) {
        this.id = Objects.requireNonNull(id);
        this.index = index;
    }

    public LatticeNodeId getId() {
        return id;
    }

    public int getIndex() {
        return index;
    }

    public boolean isSelected() {
        return selected;
    }

    public void setSelected(boolean selected) {
        this.selected = selected;
    }

    public double getBenefit() {
        return benefit;
    }

    public void setBenefit(double benefit) {
        this.benefit = benefit;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }
}