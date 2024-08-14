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

package com.starrocks.catalog.combinator;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class AggStateCombinator extends ScalarFunction  {
    private static final Logger LOG = LogManager.getLogger(AggStateCombinator.class);

    /**
     * DEFINE immediate_type {agg_func}_state(arg_types)
     * DESC:
     *  input: agg function's arg types
     *  output: immediate_type with agg_state_type
     */
    public AggStateCombinator(String funcName, List<Type> argTypes, Type intermediateType) {
        super(new FunctionName(funcName + FunctionSet.AGG_STATE_SUFFIX), argTypes, intermediateType, false);
    }

    public static Optional<Function> of(AggregateFunction aggFunc) {
        try {
            Type intermediateType = aggFunc.getIntermediateTypeOrReturnType();
            ScalarFunction aggStateFunc = new AggStateCombinator(aggFunc.functionName(), Arrays.asList(aggFunc.getArgs()),
                    intermediateType);
            aggStateFunc.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateFunc.setPolymorphic(aggFunc.isPolymorphic());
            aggStateFunc.setAggStateDesc(new AggStateDesc(aggFunc));
            LOG.info("Register agg state function: {}", aggStateFunc.functionName());
            return Optional.of(aggStateFunc);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateCombinator for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(functionName());
        sb.append("(");
        for (int i = 0; i < getNumArgs(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(getArgs()[i].toSql());
        }
        sb.append(")");
        return sb.toString();
    }
}
