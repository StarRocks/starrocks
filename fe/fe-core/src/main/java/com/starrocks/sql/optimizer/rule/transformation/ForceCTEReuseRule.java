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

import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.NonDeterministicVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

/**
 * Force cte reuse to avoid producing wrong result in the following cases:
 * 1. The opt expression contains non-deterministic function
 * 2. The opt expression contains LIMIT without ORDER BY (unstable result order)
 */
public class ForceCTEReuseRule extends TransformationRule {
    public ForceCTEReuseRule() {
        super(RuleType.TF_FORCE_CTE_REUSE,
                Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        boolean shouldForceReuse = false;

        // Always force reuse for non-deterministic functions
        if (hasNonDeterministicFunction(input)) {
            shouldForceReuse = true;
        }

        // Force reuse for LIMIT without ORDER BY if enabled by session variable
        if (context.getSessionVariable().isCboCTEForceReuseLimitWithoutOrderBy() && hasLimitWithoutOrderBy(input)) {
            shouldForceReuse = true;
        }

        if (shouldForceReuse) {
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) input.getOp();
            CTEContext cteContext = context.getCteContext();
            int cteId = produce.getCteId();
            cteContext.addForceCTE(cteId);
        }

        return Collections.emptyList();
    }

    private boolean hasNonDeterministicFunction(OptExpression root) {
        return root.getOp().accept(new NonDeterministicVisitor(), root, null);
    }

    /**
     * Check if the opt expression contains LIMIT without ORDER BY.
     * If a CTE has LIMIT without ORDER BY, inline it may cause different results
     * in different consume points due to unstable row order.
     */
    private boolean hasLimitWithoutOrderBy(OptExpression root) {
        LimitWithoutOrderByVisitor visitor = new LimitWithoutOrderByVisitor();
        return root.getOp().accept(visitor, root, null);
    }

    /**
     * Visitor to check if there's a LogicalLimitOperator (LIMIT without ORDER BY)
     * in the expression tree. Since Limit will merge with TopN, checking for
     * LogicalLimitOperator is sufficient.
     */
    private static class LimitWithoutOrderByVisitor extends OptExpressionVisitor<Boolean, Void> {
        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            // Visit children to check for LIMIT without ORDER BY
            for (OptExpression child : optExpression.getInputs()) {
                if (child.getOp().accept(this, child, null)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalLimit(OptExpression optExpression, Void context) {
            // Found LogicalLimitOperator, which means LIMIT without ORDER BY
            // This is unstable and should force CTE reuse
            return true;
        }
    }
}
