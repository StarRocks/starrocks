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
 * Execution-plan clone expression (deep-copies a column value).
 */
public class ExecClone extends ExecExpr {

    public ExecClone(Type type, ExecExpr child) {
        super(type, List.of(child));
    }

    private ExecClone(ExecClone other) {
        super(other.type, other.cloneChildren());
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable();
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.CLONE_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        // CLONE_EXPR has no extra fields
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecClone(this, context);
    }

    @Override
    public ExecClone clone() {
        return new ExecClone(this);
    }
}
