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


/**
 * DESC: immediate_type {agg_func}_state(arg_types)
 *  input type  : aggregate function's argument types
 *  return type : aggregate function's immediate type
 */
public final class AggStateCombinator extends ScalarFunction  {
    private static final Logger LOG = LogManager.getLogger(AggStateCombinator.class);

    public AggStateCombinator(FunctionName functionName, List<Type> argTypes, Type intermediateType) {
        super(functionName, argTypes, intermediateType, false);
    }

    public AggStateCombinator(AggStateCombinator other) {
        super(other);
        this.setBinaryType(TFunctionBinaryType.BUILTIN);
        this.setPolymorphic(other.isPolymorphic());
        this.setAggStateDesc(other.aggStateDesc.clone());
    }

    public static Optional<AggStateCombinator> of(AggregateFunction aggFunc) {
        try {
            Type intermediateType = aggFunc.getIntermediateTypeOrReturnType().clone();
            FunctionName funcName = new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_SUFFIX);
            AggStateCombinator aggStateFunc = new AggStateCombinator(funcName, Arrays.asList(aggFunc.getArgs()),
                    intermediateType);
            aggStateFunc.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateFunc.setPolymorphic(aggFunc.isPolymorphic());

            AggStateDesc aggStateDesc = new AggStateDesc(aggFunc);
            aggStateFunc.setAggStateDesc(aggStateDesc);
            // `agg_state` function's type will contain agg state desc.
            intermediateType.setAggStateDesc(aggStateDesc);
            // use agg state desc's nullable as `agg_state` function's nullable
            aggStateFunc.setIsNullable(aggStateDesc.getResultNullable());
            return Optional.of(aggStateFunc);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateCombinator for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public Function copy() {
        return new AggStateCombinator(this);
    }

    @Override
    public ScalarFunction withNewTypes(List<Type> newArgTypes, Type newRetType) {
        // NOTE: It's fine that only changes agg state function's arg types and return type but inner agg state desc's,
        // since FunctionAnalyzer will adjust it later.
        AggStateCombinator newFn = new AggStateCombinator(this.getFunctionName(), newArgTypes, newRetType);
        newFn.setLocation(this.getLocation());
        newFn.setSymbolName(this.getSymbolName());
        newFn.setPrepareFnSymbol(this.getPrepareFnSymbol());
        newFn.setCloseFnSymbol(this.getCloseFnSymbol());
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
