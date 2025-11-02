// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.ArrayList;
import java.util.List;

public class ExpressionPartitionDesc extends PartitionDesc {

    private Expr expr;
    // If this value is not null, the type of the partition is different from the type of the partition field.
    private Type partitionType = null;
    // range partition desc == null means this must be materialized view
    private RangePartitionDesc rangePartitionDesc = null;

    private static final List<String> AUTO_PARTITION_SUPPORT_FUNCTIONS =
            Lists.newArrayList(FunctionSet.TIME_SLICE, FunctionSet.DATE_TRUNC);

    public ExpressionPartitionDesc(RangePartitionDesc rangePartitionDesc, Expr expr) {
        super(expr.getPos());
        this.rangePartitionDesc = rangePartitionDesc;
        this.expr = expr;
    }

    public ExpressionPartitionDesc(Expr expr) {
        super(expr.getPos());
        this.expr = expr;
    }

    @Override
    public String toString() {
        return "PARTITION BY " + expr.toSql();
    }

    public RangePartitionDesc getRangePartitionDesc() {
        return rangePartitionDesc;
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

    public Type getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(Type partitionType) {
        this.partitionType = partitionType;
    }


}
