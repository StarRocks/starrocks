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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class LogicalPlan {
    private final OptExprBuilder root;
    private final List<ColumnRefOperator> outputColumn;
    private final List<ColumnRefOperator> correlation;

    public LogicalPlan(OptExprBuilder root, List<ColumnRefOperator> outputColumns,
                       List<ColumnRefOperator> correlation) {
        this.root = root;
        this.outputColumn = outputColumns;
        this.correlation = correlation;
    }

    public OptExpression getRoot() {
        return root.getRoot();
    }

    public OptExprBuilder getRootBuilder() {
        return root;
    }

    public List<ColumnRefOperator> getOutputColumn() {
        return outputColumn;
    }

    public List<ColumnRefOperator> getCorrelation() {
        return correlation;
    }
}