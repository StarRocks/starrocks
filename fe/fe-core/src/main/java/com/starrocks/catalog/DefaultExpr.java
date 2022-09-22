// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;

public class DefaultExpr {
<<<<<<< HEAD
=======

    public static final Set<String> SUPPORTED_DEFAULT_FNS = ImmutableSet.of("now()", "uuid()", "uuid_numeric()");

>>>>>>> af77b25f8 (Fix bug default current_timestamp lose efficacy (#11545))
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
