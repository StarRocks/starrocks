// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;

import java.util.ArrayList;

public class ValueList implements ParseNode {
    private final ArrayList<Expr> row;

    public ValueList(ArrayList<Expr> row) {
        this.row = row;
    }

    public ArrayList<Expr> getRow() {
        return row;
    }
}
