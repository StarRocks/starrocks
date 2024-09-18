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
 * Merge combinator for aggregate function to merge the agg state to return the final result of aggregate function.
 * DESC: return_type {agg_func}_merge(immediate_type)
 *  input type          : aggregate function's immediate_type
 *  intermediate type   : aggregate function's immediate_type
 *  return type         : aggregate function's return type
 */
public final class AggStateMergeCombinator extends AggregateFunction {
    private static final Logger LOG = LogManager.getLogger(AggStateMergeCombinator.class);

    public AggStateMergeCombinator(FunctionName functionName, Type intermediateType, Type returnType) {
        super(functionName, ImmutableList.of(intermediateType), returnType, intermediateType, false);
    }

    public AggStateMergeCombinator(AggStateMergeCombinator other) {
        super(other);
        this.setBinaryType(TFunctionBinaryType.BUILTIN);
        this.setPolymorphic(other.isPolymorphic());
        this.setAggStateDesc(other.aggStateDesc.clone());
    }

    public static Optional<AggStateMergeCombinator> of(AggregateFunction aggFunc) {
        try {
            Type imtermediateType = aggFunc.getIntermediateTypeOrReturnType();
            FunctionName functionName = new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_MERGE_SUFFIX);
            AggStateMergeCombinator aggStateMergeFunc =
                    new AggStateMergeCombinator(functionName, imtermediateType, aggFunc.getReturnType());
            aggStateMergeFunc.setBinaryType(TFunctionBinaryType.BUILTIN);
            aggStateMergeFunc.setPolymorphic(aggFunc.isPolymorphic());
            aggStateMergeFunc.setAggStateDesc(new AggStateDesc(aggFunc));
            LOG.info("Register agg state function: {}", aggStateMergeFunc.functionName());
            return Optional.of(aggStateMergeFunc);
        } catch (Exception e) {
            LOG.warn("Failed to create AggStateMergeCombinator for function: {}", aggFunc.functionName(), e);
            return Optional.empty();
        }
    }

    @Override
    public Function copy() {
        return new AggStateMergeCombinator(this);
    }

    @Override
    public AggregateFunction withNewTypes(List<Type> newArgTypes, Type newRetType) {
        // NOTE: It's fine that only changes agg state function's arg types and return type but inner agg state desc's,
        // since FunctionAnalyzer will adjust it later.
        AggStateMergeCombinator newFn = new AggStateMergeCombinator(this.getFunctionName(), newArgTypes.get(0), newRetType);
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
