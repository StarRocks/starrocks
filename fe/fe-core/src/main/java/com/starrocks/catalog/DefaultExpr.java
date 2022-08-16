// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

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
}
