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

import com.google.api.client.util.Lists;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TAggStateDesc;
import com.starrocks.thrift.TFunctionVersion;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * AggStateDesc is used to get the aggregate function which's used for common aggregate function state. 
 * Aggregate function's state needs to find the associated aggregate function, to get the aggregate function, 
 * we need to know the function name, return type, argument types and result nullable.
 */
public class AggStateDesc {
    // aggregate function's name
    @SerializedName(value = "functionName")
    private final String functionName;
    // aggregate function's argument types
    @SerializedName(value = "argTypes")
    private final List<Type> argTypes;
    // aggregate function's return type
    @SerializedName(value = "returnType")
    private final Type returnType;
    // aggregate function's result nullable
    @SerializedName(value = "resultNullable")
    private final Boolean resultNullable;

    public AggStateDesc(AggregateFunction aggFunc) {
        this(aggFunc.functionName(), aggFunc.getReturnType(), Arrays.asList(aggFunc.getArgs()));
    }

    public AggStateDesc(String functionName,
                        Type returnType,
                        List<Type> argTypes) {
        Preconditions.checkNotNull(functionName, "functionName should not be null");
        Preconditions.checkNotNull(returnType, "returnType should not be null");
        Preconditions.checkNotNull(argTypes, "argTypes should not be null");
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = argTypes;
        this.resultNullable = isAggFuncResultNullable(functionName);
    }

    private boolean isAggFuncResultNullable(String functionName) {
        // To be more compatible, always set result nullable to true here. This may decrease the performance of runtime
        // but can be more compatible with different aggregate functions and inputs.
        // this.resultNullable = !FunctionSet.alwaysReturnNonNullableFunctions.contains(functionName);
        if (FunctionSet.COUNT.equalsIgnoreCase(functionName)) {
            return false;
        } else {
            return true;
        }
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Type getReturnType() {
        return returnType;
    }

    public Boolean getResultNullable() {
        return resultNullable;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(functionName, argTypes, returnType, resultNullable);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateDesc)) {
            return false;
        }
        AggStateDesc other = (AggStateDesc) o;
        // result nullable
        if (resultNullable != other.resultNullable) {
            return false;
        }
        // func name
        if (!functionName.equalsIgnoreCase(other.functionName)) {
            return false;
        }
        // return type
        if (!returnType.equals(other.returnType)) {
            return false;
        }
        // arg types
        int subTypeNumber = argTypes.size();
        if (subTypeNumber != other.argTypes.size()) {
            return false;
        }
        for (int i = 0; i < subTypeNumber; i++) {
            if (!argTypes.get(i).equals(other.argTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    public TAggStateDesc toThrift() {
        // wrapper extra data type
        TAggStateDesc tAggStateDesc = new TAggStateDesc();
        tAggStateDesc.setAgg_func_name(Function.rectifyFunctionName(functionName));
        for (Type argType : argTypes) {
            TTypeDesc tTypeDesc = new TTypeDesc();
            tTypeDesc.setTypes(new ArrayList<TTypeNode>());
            argType.toThrift(tTypeDesc);
            tAggStateDesc.addToArg_types(tTypeDesc);
        }
        tAggStateDesc.setResult_nullable(resultNullable);
        tAggStateDesc.setFunc_version(TFunctionVersion.RUNTIME_FILTER_SERIALIZE_VERSION_3.getValue());

        // ret type
        TTypeDesc tTypeDesc = new TTypeDesc();
        tTypeDesc.setTypes(new ArrayList<TTypeNode>());
        returnType.toThrift(tTypeDesc);
        tAggStateDesc.setRet_type(tTypeDesc);
        Preconditions.checkState(!tTypeDesc.types.isEmpty());

        return tAggStateDesc;
    }

    public AggStateDesc clone() {
        return new AggStateDesc(functionName, returnType, Lists.newArrayList(argTypes));
    }

    /**
     * @return associated analyzed aggregate function
     * @throws AnalysisException: when the aggregate function is not found or not an aggregate function
     */
    public AggregateFunction getAggregateFunction() throws AnalysisException {
        FunctionParams params = new FunctionParams(false, Lists.newArrayList());
        Type[] argumentTypes = argTypes.toArray(Type[]::new);
        Boolean[] isArgumentConstants = argTypes.stream().map(x -> false).toArray(Boolean[]::new);
        Function result = FunctionAnalyzer.getAnalyzedAggregateFunction(ConnectContext.get(),
                this.functionName, params, argumentTypes, isArgumentConstants, NodePosition.ZERO);
        if (result == null) {
            throw new AnalysisException(String.format("AggStateType function %s with input %s not found", functionName,
                    argTypes));
        }
        if (!(result instanceof AggregateFunction)) {
            throw new AnalysisException(String.format("AggStateType function %s with input %s found but not an aggregate " +
                    "function", functionName, argTypes));
        }
        return (AggregateFunction) result;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(functionName).append("(");
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(argTypes.get(i).toSql());
        }
        sb.append(")");
        return sb.toString();
    }
}