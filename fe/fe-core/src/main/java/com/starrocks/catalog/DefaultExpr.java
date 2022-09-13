// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;

public class DefaultExpr {
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
        if ("uuid()".equals(expr)) {
            FunctionCallExpr functionCallExpr = new FunctionCallExpr(new FunctionName("uuid"), Lists.newArrayList());
            functionCallExpr.setType(Type.VARCHAR);
            functionCallExpr.setFn(Expr.getBuiltinFunction("uuid", new Type[]{}, Function.CompareMode.IS_IDENTICAL));
            return functionCallExpr;
        }
        return null;
    }
}
