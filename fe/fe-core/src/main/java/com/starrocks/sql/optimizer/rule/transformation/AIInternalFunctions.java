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

import com.google.common.base.Preconditions;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;

import java.util.List;

/** Resolved internal functions are constructed per call and never registered for SQL lookup. */
public final class AIInternalFunctions {
    private AIInternalFunctions() {
    }

    public static AggregateFunction createTextCollector() {
        AggregateFunction fn = AggregateFunction.createBuiltin(VectorizedBuiltinFunctions.AI_TEXT_COLLECTOR_NAME,
                List.of(VarcharType.VARCHAR), VarbinaryType.VARBINARY, VarbinaryType.VARBINARY,
                false, false, false);
        fn.setFunctionId(VectorizedBuiltinFunctions.AI_TEXT_COLLECTOR_ID);
        fn.setId(VectorizedBuiltinFunctions.AI_TEXT_COLLECTOR_ID);
        fn.setUserVisible(false);
        fn.setIsNullable(true);
        return fn;
    }

    public static ScalarFunction createReducer(AggregateFunction publicFunction, List<Type> argumentTypes) {
        var descriptor = VectorizedBuiltinFunctions.getAIAggregateLoweringDescriptor(publicFunction.getFunctionId());
        Preconditions.checkState(publicFunction.isAI() && descriptor != null, "Expected semantic AI aggregate");
        ScalarFunction fn = ScalarFunction.createVectorizedBuiltin(descriptor.reducerFunctionId(),
                descriptor.reducerFunctionName(), argumentTypes, false, publicFunction.getReturnType());
        fn.setBinaryType(TFunctionBinaryType.AI);
        fn.setUserVisible(false);
        fn.setIsNullable(true);
        return fn;
    }
}
