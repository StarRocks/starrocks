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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Combine combinator for aggregate function to combine the agg state to return the intermediate result of aggregate function.
 * DESC: intermediate_type {agg_func}_combine(arg_types)
 *  input type          : aggregate function's arg_types
 *  intermediate type   : aggregate function's intermediate_type
 *  return type         : aggregate function's intermediate_type
 */
public final class AggStateCombineCombinator extends AggregateFunction {
    private static final Logger LOG = LogManager.getLogger(AggStateCombineCombinator.class);

    public AggStateCombineCombinator(FunctionName functionName, List<Type> argTypes, Type intermediateType) {
        super(functionName, argTypes, intermediateType, intermediateType, false);
    }

    public AggStateCombineCombinator(AggStateCombineCombinator other) {
        super(other);
        this.setBinaryType(TFunctionBinaryType.BUILTIN);
        this.setPolymorphic(other.isPolymorphic());
        this.setAggStateDesc(other.aggStateDesc.clone());
    }

    public static Optional<AggStateCombineCombinator> of(AggregateFunction aggFunc) {
        try {
            Type intermediateType = aggFunc.getIntermediateTypeOrReturnType().clone();
            FunctionName functionName = new FunctionName(AggStateUtils.aggStateCombineFunctionName(aggFunc.functionName()));
            List<Type> argTypes = Arrays.asList(aggFunc.getArgs());
            AggStateCombineCombinator aggStateCombineFunc = new AggStateCombineCombinator(functionName, argTypes,
                    intermediateType);
            aggStateCombineFunc.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateCombineFunc.setPolymorphic(aggFunc.isPolymorphic());
            AggStateDesc aggStateDesc;
            if (aggFunc.getAggStateDesc() != null) {
                aggStateDesc = aggFunc.getAggStateDesc().clone();
            } else {
                aggStateDesc = new AggStateDesc(aggFunc);
            }
            aggStateCombineFunc.setAggStateDesc(aggStateDesc);
            // set agg state desc for the function's result type so can be used as the later agg state functions.
            intermediateType.setAggStateDesc(aggStateDesc);
            // use agg state desc's nullable as `agg_state` function's nullable
            aggStateCombineFunc.setIsNullable(aggStateDesc.getResultNullable());
            return Optional.of(aggStateCombineFunc);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateCombineCombinator for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public Function copy() {
        return new AggStateCombineCombinator(this);
    }

    @Override
    public AggregateFunction withNewTypes(List<Type> newArgTypes, Type newRetType) {
        // NOTE: It's fine that only changes agg state function's arg types and return type but inner agg state desc's,
        // since FunctionAnalyzer will adjust it later.
        AggStateCombineCombinator newFn = new AggStateCombineCombinator(this.getFunctionName(), newArgTypes, newRetType);
        newFn.setFunctionId(this.getFunctionId());
        newFn.setChecksum(this.getChecksum());
        newFn.setBinaryType(this.getBinaryType());
        newFn.setHasVarArgs(this.hasVarArgs());
        newFn.setId(this.getId());
        newFn.setUserVisible(this.isUserVisible());
        newFn.setAggStateDesc(this.getAggStateDesc());
        newFn.setPolymorphic(this.isPolymorphic());
        return newFn;
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
