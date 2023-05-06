// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.DdlException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExpressionPartitionDesc extends PartitionDesc {

    // must be type of SlotRef or FunctionCallRef
    private Expr expr;
    private RangePartitionDesc rangePartitionDesc = null;
    // No entry created in 2.5, just for compatibility
    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

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

    public RangePartitionDesc getRangePartitionDesc() {
        return rangePartitionDesc;
    }

    public boolean isFunction() {
        return expr instanceof FunctionCallExpr;
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> columns, Map<String, Long> partitionNameToId,
                                         boolean isTemp, boolean isExprPartition)
            throws DdlException {
        // we will support other PartitionInto in the future
        PartitionType partitionType = PartitionType.RANGE;
        if (isExprPartition)  {
            partitionType = PartitionType.EXPR_RANGE;
        }
        return new ExpressionRangePartitionInfo(Arrays.asList(expr), columns, partitionType);
    }

}
