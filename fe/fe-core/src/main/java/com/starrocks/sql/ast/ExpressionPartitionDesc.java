// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.SlotRef;

public class ExpressionPartitionDesc extends PartitionDesc {

    //must be column
    private SlotRef slotRef;
    //must be type of SlotRef or FunctionCallRef
    private Expr expr;
    // if expression is type of FunctionCallExpr, isFunc = true
    // else isFunc = false
    private boolean isFunction;

    public ExpressionPartitionDesc(SlotRef slotRef, Expr expr, boolean isFunc) {
        this.slotRef = slotRef;
        this.expr = expr;
        this.isFunction = isFunc;
    }

    public SlotRef getSlotRef() {
        return slotRef;
    }

    public void setSlotRef(SlotRef slotRef) {
        this.slotRef = slotRef;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    public boolean isFunction() {
        return isFunction;
    }

    public void setFunction(boolean function) {
        isFunction = function;
    }
}
