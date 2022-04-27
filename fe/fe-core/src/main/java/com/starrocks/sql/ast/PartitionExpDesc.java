// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.SlotRef;

import java.util.List;

public class PartitionExpDesc extends PartitionDesc {

    private List<SlotRef> slotRefs;
    //expr in outputExpression
    private List<Expr> exprs;

    public PartitionExpDesc(List<SlotRef> slotRefs) {
        this.slotRefs = slotRefs;
        this.exprs = Lists.newArrayList();
    }

    public List<SlotRef> getSlotRefs() {
        return slotRefs;
    }

    public List<Expr> getExprs() {
        return exprs;
    }

    public void setExprs(List<Expr> exprs) {
        this.exprs = exprs;
    }
}
