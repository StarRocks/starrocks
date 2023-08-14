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

package com.starrocks.catalog;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

// MvPlanContext is a item that stores metadata related to the materialized view rewrite context.
// This cache is used during the FE's lifecycle to improve the performance of materialized view
// rewriting operations.
// By caching the metadata related to the materialized view rewrite context,
// subsequent materialized view rewriting operations can avoid recomputing this metadata,
// which can save time and resources.
public class MvPlanContext {
    // mv's logical plan
    private final OptExpression logicalPlan;

    // mv plan's output columns, used for mv rewrite
    private final List<ColumnRefOperator> outputColumns;

    // column ref factory used when compile mv plan
    private final ColumnRefFactory refFactory;

    // indicate whether this mv is a SPJG plan
    // if not, we do not store other fields to save memory,
    // because we will not use other fields
    private boolean isValidMvPlan;

    // mv's logical plan
    private OptExpression logicalPlanWithView;

    public MvPlanContext() {
        this.logicalPlan = null;
        this.outputColumns = null;
        this.refFactory = null;
        this.isValidMvPlan = false;
    }

    public MvPlanContext(
            OptExpression logicalPlan,
            List<ColumnRefOperator> outputColumns,
            ColumnRefFactory refFactory) {
        this.logicalPlan = logicalPlan;
        this.outputColumns = outputColumns;
        this.refFactory = refFactory;
        this.isValidMvPlan = true;
    }

    public OptExpression getLogicalPlan() {
        return logicalPlan;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public ColumnRefFactory getRefFactory() {
        return refFactory;
    }

    public boolean isValidMvPlan() {
        return isValidMvPlan;
    }

    public void setLogicalPlanWithView(OptExpression logicalPlanWithView) {
        this.logicalPlanWithView = logicalPlanWithView;
    }

    public OptExpression getLogicalPlanWithView() {
        return logicalPlanWithView;
    }
}
