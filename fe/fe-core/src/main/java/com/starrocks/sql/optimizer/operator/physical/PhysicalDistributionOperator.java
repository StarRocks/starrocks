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


package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.List;
import java.util.Set;

public class PhysicalDistributionOperator extends PhysicalOperator {
    public PhysicalDistributionOperator(DistributionSpec spec) {
        super(OperatorType.PHYSICAL_DISTRIBUTION, spec);
    }

    public void setDistributionSpec(DistributionSpec spec) {
        this.distributionSpec = spec;
    }

    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(
            List<Pair<Integer, ColumnDict>> globalDicts) {
        this.globalDicts = globalDicts;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDistribution(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalDistribution(optExpression, context);
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return true;
    }

    @Override
    public String toString() {
        return String.format("[optType: %s, distributionSpec: %s]", opType, distributionSpec);
    }
}
