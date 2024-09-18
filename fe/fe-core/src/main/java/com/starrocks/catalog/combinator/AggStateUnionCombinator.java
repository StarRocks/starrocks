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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Union combinator for aggregate function to union the agg state to return the immediate result of aggregate function.
 * DESC: immediate_type {agg_func}_union(immediate_type)
 *  input type          : aggregate function's immediate_type
 *  intermediate type   : aggregate function's immediate_type
 *  return type         : aggregate function's immediate_type
 */
public final class AggStateUnionCombinator extends AggregateFunction {
    private static final Logger LOG = LogManager.getLogger(AggStateUnionCombinator.class);

    public AggStateUnionCombinator(FunctionName functionName, Type intermediateType) {
        super(functionName, ImmutableList.of(intermediateType), intermediateType, intermediateType, false);
    }

    public AggStateUnionCombinator(AggStateUnionCombinator other) {
        super(other);
        this.setBinaryType(TFunctionBinaryType.BUILTIN);
        this.setPolymorphic(other.isPolymorphic());
        this.setAggStateDesc(other.aggStateDesc.clone());
    }

    public static Optional<AggStateUnionCombinator> of(AggregateFunction aggFunc) {
        try {
            Type intermediateType = aggFunc.getIntermediateTypeOrReturnType();
            FunctionName functionName = new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_UNION_SUFFIX);
            AggStateUnionCombinator aggStateUnionFunc =
                    new AggStateUnionCombinator(functionName, intermediateType);
            aggStateUnionFunc.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateUnionFunc.setPolymorphic(aggFunc.isPolymorphic());
            aggStateUnionFunc.setAggStateDesc(new AggStateDesc(aggFunc));
            LOG.info("Register agg state function: {}", aggStateUnionFunc.functionName());
            return Optional.of(aggStateUnionFunc);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateUnionCombinator for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public Function copy() {
        return new AggStateUnionCombinator(this);
    }

    @Override
    public AggregateFunction withNewTypes(List<Type> newArgTypes, Type newRetType) {
        // NOTE: It's fine that only changes agg state function's arg types and return type but inner agg state desc's,
        // since FunctionAnalyzer will adjust it later.
        AggStateUnionCombinator newFn = new AggStateUnionCombinator(this.getFunctionName(), newRetType);
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
