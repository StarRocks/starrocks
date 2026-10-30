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
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan collection element access (array[index] or map[key]).
 */
public class ExecCollectionElement extends ExecExpr {
    private final boolean checkOutOfBounds;

    public ExecCollectionElement(Type type, boolean checkOutOfBounds, List<ExecExpr> children) {
        super(type, children);
        this.checkOutOfBounds = checkOutOfBounds;
    }

    private ExecCollectionElement(ExecCollectionElement other) {
        super(other.type, other.cloneChildren());
        this.checkOutOfBounds = other.checkOutOfBounds;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean isCheckOutOfBounds() {
        return checkOutOfBounds;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        if (children.get(0).getType().isArrayType()) {
            return TExprNodeType.ARRAY_ELEMENT_EXPR;
        }
        return TExprNodeType.MAP_ELEMENT_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setCheck_is_out_of_bounds(checkOutOfBounds);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecCollectionElement(this, context);
    }

    @Override
    public ExecCollectionElement clone() {
        return new ExecCollectionElement(this);
    }
}
