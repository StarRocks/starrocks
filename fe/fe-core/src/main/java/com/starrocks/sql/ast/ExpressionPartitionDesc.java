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

    private Expr expr;
    private RangePartitionDesc rangePartitionDesc = null;

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
