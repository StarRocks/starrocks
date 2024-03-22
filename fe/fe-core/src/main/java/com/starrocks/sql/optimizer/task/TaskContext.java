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


package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;

import java.util.Collections;
import java.util.List;

// The context for optimizer task
public class TaskContext {
    private final OptimizerContext optimizerContext;
    private final PhysicalPropertySet requiredProperty;
    private ColumnRefSet requiredColumns;
    private double upperBoundCost;
    private List<LogicalOlapScanOperator> allLogicalOlapScanOperators;
    private List<PhysicalOlapScanOperator> allPhysicalOlapScanOperators;

    public TaskContext(OptimizerContext context,
                       PhysicalPropertySet physicalPropertySet,
                       ColumnRefSet requiredColumns,
                       double cost) {
        this.optimizerContext = context;
        this.requiredProperty = physicalPropertySet;
        this.requiredColumns = requiredColumns;
        this.upperBoundCost = cost;
        this.allLogicalOlapScanOperators = Collections.emptyList();
        this.allPhysicalOlapScanOperators = Collections.emptyList();
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public double getUpperBoundCost() {
        return upperBoundCost;
    }

    public PhysicalPropertySet getRequiredProperty() {
        return requiredProperty;
    }

    public ColumnRefSet getRequiredColumns() {
        return requiredColumns;
    }

    public void setRequiredColumns(ColumnRefSet requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    public void setUpperBoundCost(double upperBoundCost) {
        this.upperBoundCost = upperBoundCost;
    }

    public void setAllLogicalOlapScanOperators(List<LogicalOlapScanOperator> allScanOperators) {
        this.allLogicalOlapScanOperators = allScanOperators;
    }

    public List<LogicalOlapScanOperator> getAllLogicalOlapScanOperators() {
        return allLogicalOlapScanOperators;
    }

    public void setAllPhysicalOlapScanOperators(List<PhysicalOlapScanOperator> allScanOperators) {
        this.allPhysicalOlapScanOperators = allScanOperators;
    }

    public List<PhysicalOlapScanOperator> getAllPhysicalOlapScanOperators() {
        return allPhysicalOlapScanOperators;
    }
}
