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

import com.starrocks.thrift.TCaseExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/**
 * Execution-plan CASE/WHEN expression.
 */
public class ExecCaseWhen extends ExecExpr {
    private final boolean hasCase;
    private final boolean hasElse;

    public ExecCaseWhen(Type type, boolean hasCase, boolean hasElse, List<ExecExpr> children) {
        super(type, children);
        this.hasCase = hasCase;
        this.hasElse = hasElse;
    }

    private ExecCaseWhen(ExecCaseWhen other) {
        super(other.type, other.cloneChildren());
        this.hasCase = other.hasCase;
        this.hasElse = other.hasElse;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean hasCase() {
        return hasCase;
    }

    public boolean hasElse() {
        return hasElse;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.CASE_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.case_expr = new TCaseExpr(hasCase, hasElse);
        node.setChild_type(TypeSerializer.toThrift(children.get(0).getType().getPrimitiveType()));
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecCaseWhen(this, context);
    }

    @Override
    public ExecCaseWhen clone() {
        return new ExecCaseWhen(this);
    }
}
