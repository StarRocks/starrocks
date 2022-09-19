// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;

import java.util.Set;

public class DefaultExpr {

    public static final Set<String> SUPPORTED_DEFAULT_FNS = ImmutableSet.of("uuid()", "uuid_numeric()");

    @SerializedName("expr")
    private String expr;

    public DefaultExpr(String expr) {
        this.expr = expr;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public Expr obtainExpr() {
        if (SUPPORTED_DEFAULT_FNS.contains(expr)) {
            String functionName = expr.replace("()", "");
            FunctionCallExpr functionCallExpr = new FunctionCallExpr(new FunctionName(functionName), Lists.newArrayList());
            Function fn = Expr.getBuiltinFunction(functionName, new Type[] {}, Function.CompareMode.IS_IDENTICAL);
            functionCallExpr.setFn(fn);
            functionCallExpr.setType(fn.getReturnType());
            return functionCallExpr;
        }
        return null;
    }
}
