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

import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleId;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TSlotRef;
import com.starrocks.type.VarcharType;

/**
 * Execution-plan slot reference. Holds a resolved {@link SlotDescriptor}
 * and an optional display label.
 */
public class ExecSlotRef extends ExecExpr {
    private final SlotDescriptor desc;
    private final String label;
    private boolean nullable;

    public ExecSlotRef(SlotDescriptor desc) {
        super(desc.getType());
        this.desc = desc;
        this.label = null;
        this.nullable = desc.getIsNullable();
        // Match AST SlotRef behavior: CHAR types are converted to VARCHAR.
        // This ensures that getType() returns VARCHAR for CHAR columns, which
        // propagates correctly when used in slotDesc.setType(sortExpr.getType()).
        if (this.type.isChar()) {
            this.type = VarcharType.VARCHAR;
        }
    }

    public ExecSlotRef(String label, SlotDescriptor desc) {
        super(desc.getType());
        this.desc = desc;
        this.label = label;
        this.nullable = desc.getIsNullable();
        // Match AST SlotRef behavior: CHAR types are converted to VARCHAR.
        if (this.type.isChar()) {
            this.type = VarcharType.VARCHAR;
        }
    }

    private ExecSlotRef(ExecSlotRef other) {
        super(other.type);
        this.desc = other.desc;
        this.label = other.label;
        this.nullable = other.nullable;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public SlotDescriptor getDesc() {
        return desc;
    }

    public SlotId getSlotId() {
        return desc.getId();
    }

    public TupleId getTupleId() {
        return desc.getParent().getId();
    }

    public String getLabel() {
        return label;
    }

    @Override
    public boolean isNullable() {
        if (desc != null) {
            return desc.getIsNullable();
        }
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
        if (desc != null) {
            desc.setIsNullable(nullable);
        }
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.SLOT_REF;
    }

    @Override
    public void toThrift(TExprNode node) {
        if (desc != null) {
            if (desc.getParent() != null) {
                node.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
            } else {
                node.slot_ref = new TSlotRef(desc.getId().asInt(), 0);
            }
        } else {
            node.slot_ref = new TSlotRef(0, 0);
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecSlotRef(this, context);
    }

    @Override
    public ExecSlotRef clone() {
        return new ExecSlotRef(this);
    }
}
