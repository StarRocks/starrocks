// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

/**
 * FunctionTableRef is a Table Value Function statement can be parsed directly.
 * eg. `unnest` function is be parsed to FunctionTableRef
 */
public class FunctionTableRef extends TableRef {
    private final FunctionParams params;

    public FunctionTableRef(FunctionName fnName, String alias, FunctionParams params) {
        super(new TableName(null, fnName.getFunction()), alias);
        this.params = params;
    }

    public String getFnName() {
        return name.getTbl();
    }

    public FunctionParams getParams() {
        return params;
    }
}