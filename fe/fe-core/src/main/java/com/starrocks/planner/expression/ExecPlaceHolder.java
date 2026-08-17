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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlaceHolder;
import com.starrocks.type.Type;

/**
 * Execution-plan placeholder expression.
 * Used in global dictionary optimization and lambda expressions to represent
 * a synthetic input column.
 */
public class ExecPlaceHolder extends ExecExpr {
    private final int slotId;
    private final boolean nullable;

    public ExecPlaceHolder(int slotId, boolean nullable, Type type) {
        super(type);
        this.slotId = slotId;
        this.nullable = nullable;
    }

    private ExecPlaceHolder(ExecPlaceHolder other) {
        super(other.type);
        this.slotId = other.slotId;
        this.nullable = other.nullable;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public int getSlotId() {
        return slotId;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.PLACEHOLDER_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        TPlaceHolder placeHolder = new TPlaceHolder();
        placeHolder.setNullable(nullable);
        placeHolder.setSlot_id(slotId);
        node.setVslot_ref(placeHolder);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecPlaceHolder(this, context);
    }

    @Override
    public ExecPlaceHolder clone() {
        return new ExecPlaceHolder(this);
    }
}
