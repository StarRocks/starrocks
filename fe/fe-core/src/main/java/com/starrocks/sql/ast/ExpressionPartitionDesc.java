// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.SlotRef;

public class ExpressionPartitionDesc extends PartitionDesc {

    private SlotRef slotRef;
    //expr in outputExpression
    private Expr expr;

    public ExpressionPartitionDesc(SlotRef slotRef) {
        this.slotRef = slotRef;
    }

    public SlotRef getSlotRef() {
        return slotRef;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }
}
