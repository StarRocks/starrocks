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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.function.GenericFunction;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum SystemOperatorEvaluator {
    INSTANCE;

    private static final Logger LOG = LogManager.getLogger(ScalarOperatorEvaluator.class);
    private final Map<Function, GenericFunction> systemFunctionTables;

    SystemOperatorEvaluator() {
        systemFunctionTables = GlobalStateMgr.getCurrentState().getGenericFunctions();
    }

    public ConstantOperator evaluation(CallOperator root) {
        for (ScalarOperator child : root.getChildren()) {
            if (!OperatorType.CONSTANT.equals(child.getOpType())) {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR, "System Function's args does't match.");
            }
        }

        Function fn = root.getFunction();
        if (fn == null) {
            String msg = String.format("No matching system function: %s", root.getFnName());
            throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR, msg);
        }

        // return Null directly iff:
        // 1. Has null parameter
        // 2. Not in isNotAlwaysNullResultWithNullParamFunctions
        if (!GlobalStateMgr.getCurrentState()
                .isNotAlwaysNullResultWithNullParamFunction(fn.getFunctionName().getFunction())) {
            for (ScalarOperator op : root.getChildren()) {
                if (((ConstantOperator) op).isNull()) {
                    Type type = fn.getReturnType();
                    if (type.isDecimalV3()) {
                        return ConstantOperator.createNull(root.getType());
                    } else {
                        return ConstantOperator.createNull(fn.getReturnType());
                    }
                }
            }
        }

        List<ConstantOperator> constOperators = root.getChildren().stream()
                .filter(operator -> operator instanceof ConstantOperator)
                .map(operator -> (ConstantOperator) operator)
                .collect(Collectors.toList());

        if (constOperators.size() != root.getChildren().size()) {
            String msg = String.format("No matching system function: %s", root.getFnName());
            throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR, msg);
        }

        GenericFunction genericFunc = systemFunctionTables.get(fn);
        return genericFunc.evaluate(constOperators);
    }
}
