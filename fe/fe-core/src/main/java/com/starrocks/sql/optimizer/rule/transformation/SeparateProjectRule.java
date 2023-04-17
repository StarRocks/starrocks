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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

public class SeparateProjectRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return rewriteImpl(root);
    }

    public OptExpression rewriteImpl(OptExpression root) {
        for (int i = 0; i < root.getInputs().size(); ++i) {
            root.setChild(i, rewriteImpl(root.inputAt(i)));
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalProjectOperator) && operator.getProjection() != null) {
            Projection projection = operator.getProjection();
            operator.setProjection(null);
            return OptExpression.create(new LogicalProjectOperator(projection.getColumnRefMap()), root);
        } else {
            return root;
        }
    }
}