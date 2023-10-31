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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/AggregateFunction.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.FunctionName;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.thrift.TAggregateFunction;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.logging.log4j.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.starrocks.common.io.IOUtils.readOptionStringOrNull;
import static com.starrocks.common.io.IOUtils.writeOptionString;

/**
 * Internal representation of an aggregate function.
 * TODO: Create separate AnalyticFunction class
 */
public class AggregateFunction extends Function {
    // Set if different from retType_, null otherwise.
    @SerializedName(value = "intermediateType")
    private Type intermediateType;

    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    @SerializedName(value = "symbolName")
    private String symbolName;

    // If true, this aggregate function should ignore distinct.
    // e.g. min(distinct col) == min(col).
    // TODO: currently it is not possible for user functions to specify this. We should
    // extend the create aggregate function stmt to allow additional metadata like this.
    @SerializedName(value = "ignoresDistinct")
    private boolean ignoresDistinct;

    // True if this function can appear within an analytic expr (fn() OVER(...)).
    // TODO: Instead of manually setting this flag for all builtin aggregate functions
    // we should identify this property from the function itself (e.g., based on which
    // functions of the UDA API are implemented).
    // Currently, there is no reliable way of doing that.
    @SerializedName(value = "isAnalyticFn")
    private boolean isAnalyticFn;

    // True if this function can be used for aggregation (without an OVER() clause).
    @SerializedName(value = "isAggregateFn")
    private boolean isAggregateFn;

    // True if this function returns a non-null value on an empty input. It is used
    // primarily during the equal of scalar subqueries.
    // TODO: Instead of manually setting this flag, we should identify this
    // property from the function itself (e.g. evaluating the function on an
    // empty input in BE).
    @SerializedName(value = "returnsNonNullOnEmpty")
    private boolean returnsNonNullOnEmpty;

    public List<Boolean> getIsAscOrder() {
        return isAscOrder;
    }

    public void setIsAscOrder(List<Boolean> isAscOrder) {
        this.isAscOrder = isAscOrder;
    }

    private List<Boolean> isAscOrder;

    public List<Boolean> getNullsFirst() {
        return nullsFirst;
    }

    public void setNullsFirst(List<Boolean> nullsFirst) {
        this.nullsFirst = nullsFirst;
    }

    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private List<Boolean> nullsFirst;

    private boolean isDistinct = false;

    public void setIsDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean getIsDistinct() {
        return isDistinct;
    }

    // only used for serialization
    protected AggregateFunction() {
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes,
                             Type retType, Type intermediateType, boolean hasVarArgs) {
        this(fnName, argTypes, retType, intermediateType, hasVarArgs, false);
    }

    public AggregateFunction(FunctionName fnName, List<Type> argTypes, Type retType, Type intermediateType,
                             boolean hasVarArgs, boolean isAnalyticFn) {
        super(fnName, argTypes, retType, hasVarArgs);
        this.intermediateType =
                (intermediateType != null && intermediateType.equals(retType)) ? null : intermediateType;
        this.isAnalyticFn = isAnalyticFn;
        ignoresDistinct = false;
        isAggregateFn = true;
        returnsNonNullOnEmpty = false;
    }

    public static AggregateFunction createBuiltin(String name,
                                                  List<Type> argTypes, Type retType, Type intermediateType,
                                                  boolean ignoresDistinct,
                                                  boolean isAnalyticFn,
                                                  boolean returnsNonNullOnEmpty) {
        return createBuiltin(name, argTypes, retType, intermediateType, false, ignoresDistinct, isAnalyticFn,
                returnsNonNullOnEmpty);
    }


    public static AggregateFunction createBuiltin(String name,
                                                  List<Type> argTypes, Type retType, Type intermediateType,
                                                  boolean hasVarArgs, boolean ignoresDistinct,
                                                  boolean isAnalyticFn,
                                                  boolean returnsNonNullOnEmpty) {
        AggregateFunction fn = new AggregateFunction(new FunctionName(name),
                argTypes, retType, intermediateType, hasVarArgs);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.ignoresDistinct = ignoresDistinct;
        fn.isAnalyticFn = isAnalyticFn;
        fn.isAggregateFn = true;
        fn.returnsNonNullOnEmpty = returnsNonNullOnEmpty;
        return fn;
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
                                                          List<Type> argTypes, Type retType, Type intermediateType) {
        return createAnalyticBuiltin(name, argTypes, retType, intermediateType, true);
    }

    public static AggregateFunction createAnalyticBuiltin(String name,
                                                          List<Type> argTypes, Type retType, Type intermediateType,
                                                          boolean isUserVisible) {
        AggregateFunction fn = new AggregateFunction(new FunctionName(name),
                argTypes, retType, intermediateType, false);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.ignoresDistinct = false;
        fn.isAnalyticFn = true;
        fn.isAggregateFn = false;
        fn.returnsNonNullOnEmpty = false;
        fn.setUserVisible(isUserVisible);
        return fn;
    }

    // Used to create UDAF
    public AggregateFunction(FunctionName fnName, Type[] argTypes,
                             Type retType, boolean hasVarArgs, Type intermediateType, String location) {
        super(fnName, argTypes, retType, hasVarArgs);
        this.setLocation(new HdfsURI(location));
        this.intermediateType = (intermediateType.equals(retType)) ? null : intermediateType;
        ignoresDistinct = false;
        isAnalyticFn = true;
        isAggregateFn = true;
        returnsNonNullOnEmpty = false;
    }

    public AggregateFunction(AggregateFunction other) {
        super(other);
        intermediateType = other.intermediateType;
        ignoresDistinct = other.ignoresDistinct;
        isAnalyticFn = other.isAnalyticFn;
        isAggregateFn = other.isAggregateFn;
        returnsNonNullOnEmpty = other.returnsNonNullOnEmpty;
        symbolName = other.symbolName;
        isAscOrder = other.isAscOrder;
        nullsFirst = other.nullsFirst;
        isDistinct = other.isDistinct;
    }

    public String getSymbolName() {
        return symbolName == null ? Strings.EMPTY : symbolName;
    }

    public static class AggregateFunctionBuilder {
        TFunctionBinaryType binaryType;
        FunctionName name;
        Type[] argTypes;
        Type retType;
        boolean hasVarArgs;
        boolean isAnalyticFn;
        Type intermediateType;
        String objectFile;
        String symbolName;

        private AggregateFunctionBuilder(TFunctionBinaryType binaryType) {
            this.binaryType = binaryType;
        }

        public static AggregateFunctionBuilder createUdfBuilder(TFunctionBinaryType binaryType) {
            return new AggregateFunctionBuilder(binaryType);
        }

        public AggregateFunctionBuilder name(FunctionName name) {
            this.name = name;
            return this;
        }

        public AggregateFunctionBuilder argsType(Type[] argTypes) {
            this.argTypes = argTypes;
            return this;
        }

        public AggregateFunctionBuilder retType(Type type) {
            this.retType = type;
            return this;
        }

        public AggregateFunctionBuilder hasVarArgs(boolean hasVarArgs) {
            this.hasVarArgs = hasVarArgs;
            return this;
        }

        public AggregateFunctionBuilder isAnalyticFn(boolean isAnalyticFn) {
            this.isAnalyticFn = isAnalyticFn;
            return this;
        }

        public AggregateFunctionBuilder intermediateType(Type type) {
            this.intermediateType = type;
            return this;
        }

        public AggregateFunctionBuilder objectFile(String objectFile) {
            this.objectFile = objectFile;
            return this;
        }

        public AggregateFunctionBuilder symbolName(String symbolName) {
            this.symbolName = symbolName;
            return this;
        }

        public void setIntermediateType(Type intermediateType) {
            this.intermediateType = intermediateType;
        }

        public AggregateFunction build() {
            AggregateFunction fn =
                    new AggregateFunction(name, Lists.newArrayList(argTypes), retType, intermediateType, hasVarArgs,
                            isAnalyticFn);
            fn.setBinaryType(binaryType);
            fn.symbolName = symbolName;
            fn.setLocation(new HdfsURI(objectFile));
            return fn;
        }
    }

    public boolean isAnalyticFn() {
        return isAnalyticFn;
    }

    public void setisAnalyticFn(boolean isAnalyticFn) {
        this.isAnalyticFn = isAnalyticFn;
    }

    public boolean returnsNonNullOnEmpty() {
        return returnsNonNullOnEmpty;
    }

    /**
     * Returns the intermediate type of this aggregate function or null
     * if it is identical to the return type.
     */
    public Type getIntermediateType() {
        return intermediateType;
    }

    public void setIntermediateType(Type t) {
        intermediateType = t;
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE AGGREGATE FUNCTION ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(dbName() + "." + signatureString() + "\n")
                .append(" RETURNS " + getReturnType() + "\n")
                .append(" LOCATION '" + getLocation() + "'\n")
                .append(" SYMBOL='" + getSymbolName() + "'\n");

        if (getIntermediateType() != null) {
            sb.append(" INTERMEDIATE " + getIntermediateType() + "\n");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AggregateFunction)) {
            return false;
        }
        AggregateFunction agg = (AggregateFunction) obj;

        return Objects.equals(intermediateType, agg.intermediateType) && ignoresDistinct == agg.ignoresDistinct &&
                isAnalyticFn == agg.isAnalyticFn && isAggregateFn == agg.isAggregateFn &&
                returnsNonNullOnEmpty == agg.returnsNonNullOnEmpty && Objects.equals(symbolName, agg.symbolName)
                && isDistinct == agg.isDistinct && Objects.equals(nullsFirst, agg.getNullsFirst()) &&
                Objects.equals(isAscOrder, agg.getIsAscOrder()) && super.equals(obj);
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TAggregateFunction aggFn = new TAggregateFunction();
        aggFn.setIs_analytic_only_fn(isAnalyticFn && !isAggregateFn);
        if (intermediateType != null) {
            aggFn.setIntermediate_type(intermediateType.toThrift());
        } else {
            aggFn.setIntermediate_type(getReturnType().toThrift());
        }
        if (isAscOrder != null && !isAscOrder.isEmpty()) {
            aggFn.setIs_asc_order(isAscOrder);
        }
        if (nullsFirst != null && !nullsFirst.isEmpty()) {
            aggFn.setNulls_first(nullsFirst);
        }
        aggFn.setIs_distinct(isDistinct);

        aggFn.setSymbol(getSymbolName());
        fn.setAggregate_fn(aggFn);
        return fn;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.AGGREGATE.write(output);
        // 2. parent
        super.writeFields(output);
        // 3. self's member
        boolean hasInterType = intermediateType != null;
        output.writeBoolean(hasInterType);
        if (hasInterType) {
            ColumnType.write(output, intermediateType);
        }
        writeOptionString(output, symbolName);
        writeOptionString(output, Strings.EMPTY);
        writeOptionString(output, Strings.EMPTY);
        writeOptionString(output, Strings.EMPTY);
        writeOptionString(output, Strings.EMPTY);
        writeOptionString(output, Strings.EMPTY);
        writeOptionString(output, Strings.EMPTY);

        output.writeBoolean(ignoresDistinct);
        output.writeBoolean(isAnalyticFn);
        output.writeBoolean(isAggregateFn);
        output.writeBoolean(returnsNonNullOnEmpty);
    }

    public void readFields(DataInput input) throws IOException {
        super.readFields(input);

        if (input.readBoolean()) {
            intermediateType = ColumnType.read(input);
        }
        symbolName = readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        readOptionStringOrNull(input);
        ignoresDistinct = input.readBoolean();
        isAnalyticFn = input.readBoolean();
        isAggregateFn = input.readBoolean();
        returnsNonNullOnEmpty = input.readBoolean();
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("fid", getFunctionId() + "");
        properties.put(CreateFunctionStmt.FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.SYMBOL_KEY, symbolName == null ? "" : symbolName);
        properties.put(CreateFunctionStmt.TYPE_KEY, getBinaryType().name());
        return new Gson().toJson(properties);
    }

    @Override
    public Function copy() {
        return new AggregateFunction(this);
    }
}

