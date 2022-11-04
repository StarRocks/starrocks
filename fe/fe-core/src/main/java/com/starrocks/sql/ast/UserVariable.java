// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

public class UserVariable extends SetVar {
    public UserVariable(String variable, Expr value) {
        super(SetType.USER, variable, value);
    }
}
