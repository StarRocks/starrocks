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

package com.starrocks.sql.optimizer.property;

import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import java.util.Map;

public class ReplaceShuttle extends BaseScalarOperatorShuttle {
    private Map<ScalarOperator, ScalarOperator> replaceMap;

    public ReplaceShuttle(Map<ScalarOperator, ScalarOperator> replaceMap) {
        this.replaceMap = replaceMap;
    }

    public ScalarOperator rewrite(ScalarOperator scalarOperator) {
        ScalarOperator result = scalarOperator.accept(this, null);
        // failed to replace the scalarOperator
        if (scalarOperator.getUsedColumns().isIntersect(result.getUsedColumns())) {
            return null;
        }
        return result;
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        if (replaceMap.containsKey(variable)) {
            return replaceMap.get(variable);
        }
        return variable;
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, Void context) {
        if (replaceMap.containsKey(call)) {
            return replaceMap.get(call);
        }
        return super.visitCall(call, context);
    }
}
