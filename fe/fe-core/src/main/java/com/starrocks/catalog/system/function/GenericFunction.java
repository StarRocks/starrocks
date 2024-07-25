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

package com.starrocks.catalog.system.function;

import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.SystemFunction;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.List;

public interface GenericFunction {

    default void genericSystemFunctionCheck(FunctionCallExpr functionCallExpr) {
        if (!(functionCallExpr.getFn() instanceof SystemFunction)) {
            throw new SemanticException("Cannot convert scalar function to system funcion.", functionCallExpr.getPos());
        }
    }

    //analyze function argument
    void init(FunctionCallExpr functionCallExpr, ConnectContext context);

    //check privilege
    void prepare(FunctionCallExpr functionCallExpr, ConnectContext context);

    //execute function
    ConstantOperator evaluate(List<ConstantOperator> arguments);
}
