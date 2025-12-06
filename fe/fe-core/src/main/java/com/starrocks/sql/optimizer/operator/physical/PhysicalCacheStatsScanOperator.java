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

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalCacheStatsScanOperator;

import java.util.Map;

public class PhysicalCacheStatsScanOperator extends PhysicalScanOperator {

    private Map<Integer, String> aggColumnIdToNames;

    public PhysicalCacheStatsScanOperator() {
        super(OperatorType.PHYSICAL_CACHE_STATS_SCAN);
        this.aggColumnIdToNames = ImmutableMap.of();
    }

    public PhysicalCacheStatsScanOperator(LogicalCacheStatsScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_CACHE_STATS_SCAN, scanOperator);
        this.aggColumnIdToNames = scanOperator.getAggColumnIdToNames();
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCacheStatsScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCacheStatsScan(optExpression, context);
    }
}
