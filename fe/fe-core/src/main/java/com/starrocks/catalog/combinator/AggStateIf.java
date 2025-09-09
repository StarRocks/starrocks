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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class AggStateIf extends AggregateFunction {
    private static final Logger LOG = LogManager.getLogger(AggStateIf.class);

    public AggStateIf(FunctionName fnName, List<Type> argTypes,
                      Type retType, Type intermediateType) {
        super(fnName, argTypes, retType, intermediateType, false);
    }

    public AggStateIf(AggStateIf other) {
        super(other);
        this.setBinaryType(TFunctionBinaryType.BUILTIN);
        this.setPolymorphic(other.isPolymorphic());
        this.setAggStateDesc(other.aggStateDesc.clone());
    }

    public static Optional<AggStateIf> of(AggregateFunction aggFunc) {
        try {
            FunctionName functionName = new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_IF_SUFFIX);
            List<Type> argTypes = new ArrayList<>();
            argTypes.addAll(Arrays.asList(aggFunc.getArgs()));
            argTypes.add(Type.BOOLEAN);
            AggStateIf aggStateIf = new AggStateIf(functionName, argTypes, aggFunc.getReturnType(),
                    aggFunc.getIntermediateTypeOrReturnType());
            aggStateIf.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateIf.setPolymorphic(aggFunc.isPolymorphic());

            // agg_if's result should always be nullable, so BE can use nullable agg function
            AggStateDesc aggStateDesc = new AggStateDesc(aggFunc, true);
            aggStateIf.setAggStateDesc(aggStateDesc);
            aggStateIf.setIsNullable(aggStateDesc.getResultNullable());
            return Optional.of(aggStateIf);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateIf for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public Function copy() {
        return new AggStateIf(this);
    }

    // this is used for complex type
    // called by generatePolymorphicFunction
    @Override
    public AggregateFunction withNewTypes(List<Type> newArgTypes, Type newRetType) {
        // NOTE: It's fine that only changes agg state function's arg types and return type but inner agg state desc's,
        // since FunctionAnalyzer will adjust it later.
        AggStateIf newFn = new AggStateIf(this.getFunctionName(), newArgTypes, newRetType, this.getIntermediateType());
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
