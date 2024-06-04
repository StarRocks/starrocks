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

public class QueryBenefit {
    private final LatticeNodeId id;
    private final double weight;
    private LatticeNodeId usedLatticeId;
    private double cost;
    private double benefit;

    public QueryBenefit(LatticeNodeId id, double weight, double initialCost) {
        this.id = Objects.requireNonNull(id);
        this.weight = weight;
        this.cost = initialCost;
        this.benefit = 0.0;
    }

    public LatticeNodeId getId() {
        return this.id;
    }

    public double getWeight() {
        return weight;
    }

    public LatticeNodeId getUsedLatticeId() {
        return usedLatticeId;
    }

    public void setUsedLatticeId(LatticeNodeId usedLatticeId) {
        this.usedLatticeId = usedLatticeId;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getBenefit() {
        return benefit;
    }

    public void setBenefit(double benefit) {
        this.benefit = benefit;
    }

}
