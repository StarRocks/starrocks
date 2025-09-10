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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  OnlyJoinRule is used to match the only-join query pattern and rewrite it by mv.
 */
public class OnlyJoinRule extends BaseMaterializedViewRewriteRule {
    private static final OnlyJoinRule INSTANCE = new OnlyJoinRule();

    public OnlyJoinRule() {
        super(RuleType.TF_MV_ONLY_JOIN_RULE, Pattern.create(OperatorType.PATTERN_MULTIJOIN));
    }

    public static OnlyJoinRule getInstance() {
        return INSTANCE;
    }

    public static boolean isLogicalSPJ(OptExpression root,
                                       AtomicBoolean hasJoin) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalFilterOperator)
                && !(operator instanceof LogicalJoinOperator)) {
            return false;
        }
        if (operator instanceof LogicalJoinOperator) {
            hasJoin.set(true);
        }
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) operator;
            if (olapScanOperator.isSample()) {
                return false;
            }
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPJ(child, hasJoin)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // for only-join rule, only SPJ is supported
        AtomicBoolean hasJoin = new AtomicBoolean(false);
        if (!isLogicalSPJ(input, hasJoin) || !hasJoin.get()) {
            return false;
        }
        return super.check(input, context);
    }
}