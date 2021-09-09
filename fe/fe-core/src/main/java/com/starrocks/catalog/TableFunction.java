// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TTableFunction;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Internal representation of a table value function.
 */
public class TableFunction extends Function {
    /**
     * default column name return by table function
     */
    private final List<String> defaultColumnNames;
    private final List<Type> tableFnReturnTypes;

    public TableFunction(FunctionName fnName, List<String> defaultColumnNames, List<Type> argTypes,
                         List<Type> tableFnReturnTypes) {
        super(fnName, argTypes, Type.INVALID, false);
        this.tableFnReturnTypes = tableFnReturnTypes;
        this.defaultColumnNames = defaultColumnNames;

        setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    public static void initBuiltins(FunctionSet functionSet) {
        TableFunction tableFunction = new TableFunction(new FunctionName("unnest"), Lists.newArrayList("unnest"),
                Lists.newArrayList(Type.ANY_ARRAY), Lists.newArrayList(Type.ANY_ELEMENT));

        functionSet.addBuiltin(tableFunction);
    }

    public List<Type> getTableFnReturnTypes() {
        return tableFnReturnTypes;
    }

    public List<String> getDefaultColumnNames() {
        return defaultColumnNames;
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TTableFunction tableFn = new TTableFunction();
        tableFn.setRet_types(tableFnReturnTypes.stream().map(Type::toThrift).collect(Collectors.toList()));
        fn.setTable_fn(tableFn);
        return fn;
    }
}