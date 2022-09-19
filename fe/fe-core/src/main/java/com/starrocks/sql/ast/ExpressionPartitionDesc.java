// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.DdlException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExpressionPartitionDesc extends PartitionDesc {

    // must be type of SlotRef or FunctionCallRef
    private Expr expr;

    public ExpressionPartitionDesc(Expr expr) {
        Preconditions.checkState(expr != null);
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        Preconditions.checkState(expr != null);
        this.expr = expr;
    }

    public SlotRef getSlotRef() {
        if (expr instanceof FunctionCallExpr) {
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef) {
                    return (SlotRef) child;
                }
            }
        }
        return ((SlotRef) expr);
    }

    public boolean isFunction() {
        return expr instanceof FunctionCallExpr;
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        // we will support other PartitionInto in the future
        return new ExpressionRangePartitionInfo(Arrays.asList(expr), columns);
    }

}
