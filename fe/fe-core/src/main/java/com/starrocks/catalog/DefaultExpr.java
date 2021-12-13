// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;

public class DefaultExpr {
    @SerializedName("expr")
    private Expr expr;

    public DefaultExpr(Expr expr) {
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }
}
