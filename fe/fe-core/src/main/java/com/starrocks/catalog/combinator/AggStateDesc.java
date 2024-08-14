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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TAggStateDesc;
import com.starrocks.thrift.TFunctionVersion;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It's a wrapper for the result type with extra information for the aggregate function.
 */
public class AggStateDesc {
    // agg function's name
    @SerializedName(value = "functionName")
    private String functionName;
    // argument types
    @SerializedName(value = "argTypes")
    private List<Type> argTypes;
    // return type
    @SerializedName(value = "returnType")
    private Type returnType;
    // result nullable
    @SerializedName(value = "resultNullable")
    private Boolean resultNullable;

    public AggStateDesc(AggregateFunction aggFunc) {
        this(aggFunc.functionName(), aggFunc.getReturnType(), Arrays.asList(aggFunc.getArgs()),
                aggFunc.isNullable());
    }

    public AggStateDesc(String functionName,
                        Type returnType,
                        List<Type> argTypes,
                        Boolean resultNullable) {
        Preconditions.checkNotNull(functionName, "functionName should not be null");
        Preconditions.checkNotNull(returnType, "returnType should not be null");
        Preconditions.checkNotNull(argTypes, "argTypes should not be null");
        Preconditions.checkNotNull(resultNullable, "resultNullable should not be null");
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = argTypes;
        this.resultNullable = resultNullable;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Boolean getResultNullable() {
        return resultNullable;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(argTypes, resultNullable, functionName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateDesc)) {
            return false;
        }
        AggStateDesc other = (AggStateDesc) o;
        int subTypeNumber = argTypes.size();
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
        tAggStateDesc.setAgg_func_name(functionName);
        for (Type argType : argTypes) {
            TTypeDesc tTypeDesc = new TTypeDesc();
            tTypeDesc.setTypes(new ArrayList<TTypeNode>());
            argType.toThrift(tTypeDesc);
            tAggStateDesc.addToArg_types(tTypeDesc);
        }
        tAggStateDesc.setResult_nullable(resultNullable);
        tAggStateDesc.setFunc_version(TFunctionVersion.RUNTIME_FILTER_SERIALIZE_VERSION_2.getValue());

        // ret type
        TTypeDesc tTypeDesc = new TTypeDesc();
        tTypeDesc.setTypes(new ArrayList<TTypeNode>());
        returnType.toThrift(tTypeDesc);
        tAggStateDesc.setRet_type(tTypeDesc);
        Preconditions.checkState(!tTypeDesc.types.isEmpty());

        return tAggStateDesc;
    }

    public AggStateDesc clone() {
        return new AggStateDesc(functionName, returnType.clone(), Lists.newArrayList(argTypes), resultNullable);
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