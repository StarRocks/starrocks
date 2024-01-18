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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.structure.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TTableFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Collectors;

/**
 * Internal representation of a table value function.
 */
public class TableFunction extends Function {
    /**
     * default column name return by table function
     */
    @SerializedName(value = "defaultColumnNames")
    private List<String> defaultColumnNames;
    @SerializedName(value = "tableFnReturnTypes")
    private List<Type> tableFnReturnTypes;
    @SerializedName(value = "symbolName")
    private String symbolName = "";

    // only used for serialization


    protected TableFunction() {
    }

    public TableFunction(FunctionName fnName, List<String> argNames, List<String> defaultColumnNames, List<Type> argTypes,
                         List<Type> tableFnReturnTypes, Vector<Pair<String, Expr>> defaultArgExpr) {
        this(fnName, argNames, defaultColumnNames, argTypes, tableFnReturnTypes, defaultArgExpr, false);
    }

    public TableFunction(FunctionName fnName, List<String> defaultColumnNames, List<Type> argTypes,
                         List<Type> tableFnReturnTypes) {
        this(fnName, null, defaultColumnNames, argTypes, tableFnReturnTypes, null, false);
    }

    public TableFunction(FunctionName fnName, List<String> argNames, List<String> defaultColumnNames,
                         List<Type> argTypes, List<Type> tableFnReturnTypes, Vector<Pair<String, Expr>> defaultArgExpr,
                         boolean varArgs) {
        super(fnName, argTypes, Type.INVALID, varArgs);
        this.tableFnReturnTypes = tableFnReturnTypes;
        this.defaultColumnNames = defaultColumnNames;
        setArgNames(argNames);
        setDefaultNamedArgs(defaultArgExpr);
        setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    public TableFunction(FunctionName fnName, List<String> defaultColumnNames, List<Type> argTypes,
                         List<Type> tableFnReturnTypes, boolean varArgs) {
        super(fnName, argTypes, Type.INVALID, varArgs);
        this.tableFnReturnTypes = tableFnReturnTypes;
        this.defaultColumnNames = defaultColumnNames;
        setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    public TableFunction(TableFunction other) {
        super(other);
        defaultColumnNames = other.defaultColumnNames;
        tableFnReturnTypes = other.tableFnReturnTypes;
        symbolName = other.symbolName;
    }

    public static void initBuiltins(FunctionSet functionSet) {
        TableFunction unnest = new TableFunction(new FunctionName("unnest"), Lists.newArrayList("unnest"),
                Lists.newArrayList(Type.ANY_ARRAY), Lists.newArrayList(Type.ANY_ELEMENT), true);
        functionSet.addBuiltin(unnest);

        TableFunction jsonEach = new TableFunction(new FunctionName("json_each"), Lists.newArrayList("key", "value"),
                Lists.newArrayList(Type.JSON), Lists.newArrayList(Type.VARCHAR, Type.JSON));
        functionSet.addBuiltin(jsonEach);

        for (Type type : Lists.newArrayList(Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT, Type.LARGEINT)) {
            TableFunction func = new TableFunction(new FunctionName("subdivide_bitmap"), Lists.newArrayList("subdivide_bitmap"),
                    Lists.newArrayList(Type.BITMAP, type), Lists.newArrayList(Type.BITMAP));
            functionSet.addBuiltin(func);
        }

        TableFunction funcUnnestBitmap = new TableFunction(new FunctionName("unnest_bitmap"),
                Lists.newArrayList("unnest_bitmap"), Lists.newArrayList(Type.BITMAP), Lists.newArrayList(Type.BIGINT));
        functionSet.addBuiltin(funcUnnestBitmap);

        for (Type type : Lists.newArrayList(Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT, Type.LARGEINT)) {
            // set default arguments' const expressions in order
            Vector<Pair<String, Expr>> defaultArgs = new Vector<>();
            try {
                defaultArgs.add(new Pair("step", LiteralExpr.create("1", type)));
            } catch (AnalysisException ex) { //ignored
            }
            // for both named arguments and positional arguments
            TableFunction func = new TableFunction(new FunctionName("generate_series"),
                    Lists.newArrayList("start", "end", "step"),
                    Lists.newArrayList("generate_series"),
                    Lists.newArrayList(type, type, type),
                    Lists.newArrayList(type), defaultArgs);
            functionSet.addBuiltin(func);
        }

        TableFunction listRowsets = new TableFunction(new FunctionName("list_rowsets"),
                Lists.newArrayList("id", "segments", "rows", "size", "overlapped", "delete_predicate"),
                Lists.newArrayList(/*tablet_id*/Type.BIGINT, /*tablet_version*/Type.BIGINT),
                Lists.newArrayList(Type.BIGINT, Type.BIGINT, Type.BIGINT, Type.BIGINT, Type.BOOLEAN, Type.STRING));
        functionSet.addBuiltin(listRowsets);
    }

    public List<Type> getTableFnReturnTypes() {
        return tableFnReturnTypes;
    }

    public List<String> getDefaultColumnNames() {
        return defaultColumnNames;
    }

    public void setSymbolName(String symbolName) {
        this.symbolName = symbolName;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // 1. type
        FunctionType.TABLE.write(output);
        // 2. parent
        super.writeFields(output);
        // 3. write self
        Text.writeString(output, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        final TableFunction tableFunction = GsonUtils.GSON.fromJson(Text.readString(input), TableFunction.class);
        this.symbolName = tableFunction.symbolName;
        this.tableFnReturnTypes = tableFunction.getTableFnReturnTypes();
        this.defaultColumnNames = tableFunction.getDefaultColumnNames();
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TTableFunction tableFn = new TTableFunction();
        tableFn.setSymbol(symbolName);
        tableFn.setRet_types(tableFnReturnTypes.stream().map(Type::toThrift).collect(Collectors.toList()));
        fn.setTable_fn(tableFn);
        return fn;
    }

    @Override
    public String getProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("fid", getFunctionId() + "");
        properties.put(CreateFunctionStmt.FILE_KEY, getLocation() == null ? "" : getLocation().toString());
        properties.put(CreateFunctionStmt.MD5_CHECKSUM, checksum);
        properties.put(CreateFunctionStmt.SYMBOL_KEY, symbolName);
        properties.put(CreateFunctionStmt.TYPE_KEY, getBinaryType().name());
        return new Gson().toJson(properties);
    }

    @Override
    public Function copy() {
        return new TableFunction(this);
    }
}