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

import java.util.Optional;

public class AggStateUnionCombinator extends AggregateFunction {
    private static final Logger LOG = LogManager.getLogger(AggStateUnionCombinator.class);

    /**
     * Union combinator for aggregate function to union the agg state to return the immediate result of aggregate function.
     * </p>
     * DEFINE immediate_type {agg_func}_union(immediate_type)
     * DESC:
     *  input: immediate_type with agg_state_type
     *  immediate: immediate_type with agg_state_type
     *  output: immediate_type with agg_state_type
     */
    public AggStateUnionCombinator(String funcName, Type intermediateType) {
        super(new FunctionName(funcName + FunctionSet.AGG_STATE_UNION_SUFFIX),
                ImmutableList.of(intermediateType), intermediateType, intermediateType, false);
    }

    public static Optional<Function> of(AggregateFunction aggFunc) {
        try {
            Type intermediateType = aggFunc.getIntermediateTypeOrReturnType();
            AggregateFunction aggStateUnionFunc = new AggStateUnionCombinator(aggFunc.functionName(), intermediateType);
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
