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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TTableFunction;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.JsonType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarcharType;

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

    // not serialized
    private boolean isLeftJoin = false;

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
        super(fnName, argTypes, InvalidType.INVALID, varArgs);
        this.tableFnReturnTypes = tableFnReturnTypes;
        this.defaultColumnNames = defaultColumnNames;
        setArgNames(argNames);
        setDefaultNamedArgs(defaultArgExpr);
        setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    public TableFunction(FunctionName fnName, List<String> defaultColumnNames, List<Type> argTypes,
                         List<Type> tableFnReturnTypes, boolean varArgs) {
        super(fnName, argTypes, InvalidType.INVALID, varArgs);
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
                Lists.newArrayList(AnyArrayType.ANY_ARRAY), Lists.newArrayList(AnyElementType.ANY_ELEMENT), true);
        functionSet.addBuiltin(unnest);

        TableFunction jsonEach = new TableFunction(new FunctionName("json_each"), Lists.newArrayList("key", "value"),
                Lists.newArrayList(JsonType.JSON), Lists.newArrayList(VarcharType.VARCHAR, JsonType.JSON));
        functionSet.addBuiltin(jsonEach);

        for (Type type : Lists.newArrayList(IntegerType.TINYINT, IntegerType.SMALLINT,
                IntegerType.INT, IntegerType.BIGINT, IntegerType.LARGEINT)) {
            TableFunction func = new TableFunction(new FunctionName("subdivide_bitmap"),
                    Lists.newArrayList("subdivide_bitmap"),
                    Lists.newArrayList(BitmapType.BITMAP, type),
                    Lists.newArrayList(BitmapType.BITMAP));
            functionSet.addBuiltin(func);
        }

        TableFunction funcUnnestBitmap = new TableFunction(new FunctionName(FunctionSet.UNNEST_BITMAP),
                Lists.newArrayList(FunctionSet.UNNEST_BITMAP), Lists.newArrayList(BitmapType.BITMAP), Lists.newArrayList(
                IntegerType.BIGINT));
        functionSet.addBuiltin(funcUnnestBitmap);

        for (Type type : Lists.newArrayList(IntegerType.TINYINT, IntegerType.SMALLINT,
                IntegerType.INT, IntegerType.BIGINT, IntegerType.LARGEINT)) {
            // set default arguments' const expressions in order
            Vector<Pair<String, Expr>> defaultArgs = new Vector<>();
            try {
                defaultArgs.add(new Pair("step", LiteralExprFactory.create("1", type)));
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
                Lists.newArrayList(/*tablet_id*/IntegerType.BIGINT,
                        /*tablet_version*/IntegerType.BIGINT),
                Lists.newArrayList(IntegerType.BIGINT, IntegerType.BIGINT,
                        IntegerType.BIGINT, IntegerType.BIGINT,
                        BooleanType.BOOLEAN, StringType.STRING));
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

    public void setIsLeftJoin(boolean isLeftJoin) {
        this.isLeftJoin = isLeftJoin;
    }

    public boolean isLeftJoin() {
        return this.isLeftJoin;
    }



    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TTableFunction tableFn = new TTableFunction();
        tableFn.setSymbol(symbolName);
        tableFn.setRet_types(tableFnReturnTypes.stream().map(TypeSerializer::toThrift).collect(Collectors.toList()));
        tableFn.setIs_left_join(isLeftJoin);
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