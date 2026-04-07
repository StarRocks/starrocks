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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/**
 * Execution-plan cast expression.
 */
public class ExecCast extends ExecExpr {
    private final boolean isImplicit;

    public ExecCast(Type targetType, ExecExpr child, boolean isImplicit) {
        super(targetType, List.of(child));
        this.isImplicit = isImplicit;
    }

    private ExecCast(ExecCast other) {
        super(other.type, other.cloneChildren());
        this.isImplicit = other.isImplicit;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    @Override
    public boolean isNullable() {
        ExecExpr child = children.get(0);
        // Non-compatible casts (e.g., VARCHAR→INT) can produce NULL even from non-null input
        if (child.getType().isFullyCompatible(type)) {
            return child.isNullable();
        }
        return true;
    }

    @Override
    public boolean isSelfMonotonic() {
        // Narrowing casts can overflow to NULL, breaking monotonic order.
        // e.g., cast(bigint to tinyint) < 10: min/max may overflow, but values in between may not.
        return false;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.CAST_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setOpcode(TExprOpcode.CAST);
        Type childType = children.get(0).getType();
        if (childType.isComplexType() || childType.isDecimalOfAnyVersion()
                || childType.isChar() || childType.isVarchar()) {
            node.setChild_type_desc(TypeSerializer.toThrift(childType));
        }
        if (!childType.isComplexType()) {
            node.setChild_type(TypeSerializer.toThrift(childType.getPrimitiveType()));
        }
        if (childType.isStructType() && type.isStructType()) {
            ConnectContext ctx = ConnectContext.get();
            if (ctx != null && SqlModeHelper.check(ctx.getSessionVariable().getSqlMode(),
                    SqlModeHelper.MODE_STRUCT_CAST_BY_NAME)) {
                node.setCast_struct_by_name(true);
            }
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecCast(this, context);
    }

    @Override
    public ExecCast clone() {
        return new ExecCast(this);
    }
}
