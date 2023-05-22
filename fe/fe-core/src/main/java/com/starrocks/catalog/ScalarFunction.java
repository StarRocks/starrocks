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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ScalarFunction.java

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

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.FunctionName;
import com.starrocks.common.io.Text;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.HdfsURI;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TScalarFunction;
import org.apache.logging.log4j.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.io.IOUtils.writeOptionString;

/**
 * Internal representation of a scalar function.
 */
public class ScalarFunction extends Function {
    // The name inside the binary at location_ that contains this particular
    // function. e.g. org.example.MyUdf.class.
    @SerializedName(value = "symbolName")
    private String symbolName;
    @SerializedName(value = "prepareFnSymbol")
    private String prepareFnSymbol;
    @SerializedName(value = "closeFnSymbol")
    private String closeFnSymbol;

    // Only used for serialization
    protected ScalarFunction() {
    }

    public ScalarFunction(
            FunctionName fnName, Type[] argTypes, Type retType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
    }

    public ScalarFunction(
            FunctionName fnName, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        super(fnName, argTypes, retType, hasVarArgs);
    }

    public ScalarFunction(FunctionName fnName, List<Type> argTypes,
                          Type retType, HdfsURI location, String symbolName, String initFnSymbol,
                          String closeFnSymbol) {
        super(fnName, argTypes, retType, false);
        setLocation(location);
        setSymbolName(symbolName);
        setPrepareFnSymbol(initFnSymbol);
        setCloseFnSymbol(closeFnSymbol);
    }

    public ScalarFunction(long fid, FunctionName name, List<Type> argTypes, Type retType, boolean hasVarArgs) {
        super(fid, name, argTypes, retType, hasVarArgs);
    }

    public ScalarFunction(ScalarFunction other) {
        super(other);
        symbolName = other.symbolName;
        prepareFnSymbol = other.prepareFnSymbol;
        closeFnSymbol = other.closeFnSymbol;
    }

    public static ScalarFunction createVectorizedBuiltin(long fid,
                                                         String name, List<Type> argTypes,
                                                         boolean hasVarArgs, Type retType) {
        ScalarFunction fn = new ScalarFunction(fid, new FunctionName(name), argTypes, retType, hasVarArgs);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.setUserVisible(true);
        return fn;
    }

    /**
     * Creates a builtin scalar operator function. This is a helper that wraps a few steps
     * into one call.
     * TODO: this needs to be kept in sync with what generates the be operator
     * implementations. (gen_functions.py). Is there a better way to coordinate this.
     */
    public static ScalarFunction createBuiltinOperator(
            String name, ArrayList<Type> argTypes, Type retType) {
        return createBuiltinOperator(name, Strings.EMPTY, argTypes, retType);
    }

    public static ScalarFunction createBuiltinOperator(
            String name, String symbol, ArrayList<Type> argTypes, Type retType) {
        return createBuiltin(name, symbol, argTypes, false, retType, false);
    }

    public static ScalarFunction createBuiltin(
            String name, String symbol, ArrayList<Type> argTypes,
            boolean hasVarArgs, Type retType, boolean userVisible) {
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(name), argTypes, retType, hasVarArgs);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        fn.setUserVisible(userVisible);
        fn.symbolName = symbol;
        return fn;
    }

    public static ScalarFunction createUdf(
            FunctionName name, Type[] args,
            Type returnType, boolean isVariadic,
            TFunctionBinaryType binaryType,
            String objectFile, String symbol, String prepareFnSymbol, String closeFnSymbol) {
        ScalarFunction fn = new ScalarFunction(name, args, returnType, isVariadic);
        fn.setBinaryType(binaryType);
        fn.setUserVisible(true);
        fn.symbolName = symbol;
        fn.prepareFnSymbol = prepareFnSymbol;
        fn.closeFnSymbol = closeFnSymbol;
        fn.setLocation(new HdfsURI(objectFile));
        return fn;
    }

    public void setSymbolName(String s) {
        symbolName = s;
    }

    public void setPrepareFnSymbol(String s) {
        prepareFnSymbol = s;
    }

    public void setCloseFnSymbol(String s) {
        closeFnSymbol = s;
    }

    public String getSymbolName() {
        return symbolName == null ? Strings.EMPTY : symbolName;
    }

    public String getPrepareFnSymbol() {
        return prepareFnSymbol;
    }

    public String getCloseFnSymbol() {
        return closeFnSymbol;
    }

    @Override
    public String toSql(boolean ifNotExists) {
        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(dbName() + "." + signatureString() + "\n")
                .append(" RETURNS " + getReturnType() + "\n")
                .append(" LOCATION '" + getLocation() + "'\n")
                .append(" SYMBOL='" + getSymbolName() + "'\n");
        return sb.toString();
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TScalarFunction scalarFunction = new TScalarFunction();
        scalarFunction.setSymbol(getSymbolName());
        if (prepareFnSymbol != null) {
            scalarFunction.setPrepare_fn_symbol(prepareFnSymbol);
        }
        if (closeFnSymbol != null) {
            scalarFunction.setClose_fn_symbol(closeFnSymbol);
        }
        fn.setScalar_fn(scalarFunction);
        return fn;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.SCALAR.write(output);
        // 2. parent
        super.writeFields(output);
        // 3.symbols
        Text.writeString(output, symbolName);
        writeOptionString(output, prepareFnSymbol);
        writeOptionString(output, closeFnSymbol);
    }

    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        symbolName = Text.readString(input);
        if (input.readBoolean()) {
            prepareFnSymbol = Text.readString(input);
        }
        if (input.readBoolean()) {
            closeFnSymbol = Text.readString(input);
        }
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("fid", getFunctionId() + "");
        properties.put(CreateFunctionStmt.FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.SYMBOL_KEY, getSymbolName());
        properties.put(CreateFunctionStmt.TYPE_KEY, getBinaryType().name());
        return new Gson().toJson(properties);
    }

    @Override
    public Function copy() {
        return new ScalarFunction(this);
    }
}
