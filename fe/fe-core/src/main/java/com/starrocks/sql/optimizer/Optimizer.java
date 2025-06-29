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
package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;

public abstract class Optimizer {
    protected final OptimizerContext context;

    Optimizer(OptimizerContext context) {
        this.context = context;
    }

    public OptimizerContext getContext() {
        return context;
    }

    public OptExpression optimize(OptExpression tree, ColumnRefSet requiredColumns) {
        return optimize(tree, new PhysicalPropertySet(), requiredColumns);
    }

    public abstract OptExpression optimize(OptExpression tree, PhysicalPropertySet requiredProperty,
                                           ColumnRefSet requiredColumns);

    protected void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }
}
