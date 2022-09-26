// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.FunctionSearchDesc;

public class DropFunctionStmt extends DdlStmt {
    private final FunctionName functionName;

    public FunctionArgsDef getArgsDef() {
        return argsDef;
    }

    private final FunctionArgsDef argsDef;

    // set after analyzed
    private FunctionSearchDesc function;

    public DropFunctionStmt(FunctionName functionName, FunctionArgsDef argsDef) {
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionSearchDesc getFunction() {
        return function;
    }

    public void setFunction(FunctionSearchDesc function) {
        this.function = function;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFunctionStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
