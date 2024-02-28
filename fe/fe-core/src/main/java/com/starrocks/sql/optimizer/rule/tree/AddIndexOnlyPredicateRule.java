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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;

// This rule add predicate in physical olap scan node
// which is needed in BE's Index filtering phase
// and set index_only_filter as "true" to avoid extra expr computation costs

public class AddIndexOnlyPredicateRule implements TreeRewriteRule {
    private static final AddIndexOnlyPredicateVisitor HANDLER = new AddIndexOnlyPredicateVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(HANDLER, root, taskContext);
        return root;
    }

    private static class AddIndexOnlyPredicateVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression opt, TaskContext context) {
            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalOlapScan(OptExpression opt, TaskContext context) {
            PhysicalOlapScanOperator scan = opt.getOp().cast();
            Projection projection = scan.getProjection();
            if (projection == null || projection.getColumnRefMap() == null) {
                return null;
            }
            // if projection has function in INDEX_ONLY_FUNCTIONS
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                ScalarOperator value = entry.getValue();
                if (value instanceof CallOperator) {
                    CallOperator call = (CallOperator) value;
                    if (FunctionSet.INDEX_ONLY_FUNCTIONS.contains(call.getFnName())) {
                        // Set as index only filter
                        call.setIndexOnlyFilter(true);
                        BinaryPredicateOperator newIndexPredicate =
                                BinaryPredicateOperator.ge(call, ConstantOperator.createDouble(0));
                        scan.setPredicate(CompoundPredicateOperator.and(scan.getPredicate(), newIndexPredicate));
                        return null;
                    }
                }
            }
            return null;
        }
    }
}