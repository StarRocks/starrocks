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

package com.starrocks.sql.optimizer.rule.ivm.common;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

public class IvmRuleUtils {
    public static final String ACTION_COLUMN_NAME = "__ACTION__";
    public static final Type ACTION_COLUMN_TYPE = IntegerType.TINYINT;

    private IvmRuleUtils() {
    }

    public static boolean containsLogicalDelta(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_DELTA) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalDelta(child)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsLogicalVersion(OptExpression root) {
        if (root.getOp().getOpType() == OperatorType.LOGICAL_VERSION) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsLogicalVersion(child)) {
                return true;
            }
        }
        return false;
    }
}
