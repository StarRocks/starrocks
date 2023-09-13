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

import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;

import java.util.List;

public class PhysicalEsScanOperator extends PhysicalScanOperator {
    private final List<EsShardPartitions> selectedIndex;

    public PhysicalEsScanOperator(LogicalEsScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_ES_SCAN, scanOperator);
        this.selectedIndex = scanOperator.getSelectedIndex();
    }

    public List<EsShardPartitions> getSelectedIndex() {
        return this.selectedIndex;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalEsScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalEsScan(optExpression, context);
    }
}
