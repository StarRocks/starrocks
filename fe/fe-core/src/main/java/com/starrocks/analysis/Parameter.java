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

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.AstVisitor;

/**
 * Parameter used in prepare-statement, placeholder ? is translated into a Parameter
 */
public class Parameter extends Expr {
    private final int slotId;

    private Expr expr;

    public Parameter(int slotId) {
        this.slotId = slotId;
    }

    public Parameter(int slotId, Expr expr) {
        this.slotId = slotId;
        this.expr = expr;
    }

    public int getSlotId() {
        return slotId;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    @Override
    public Expr clone() {
        return new Parameter(slotId, expr);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitParameterExpr(this, context);
    }

    @Override
    public Type getType() {
        Type res;
        if (expr != null) {
            res = expr.getType();
        } else {
            res = super.getType();
        }
        // use STRING as default type, since string is compatible with almost all types in the type inference
        if (res.isInvalid()) {
            res = Type.STRING;
        }
        return res;
    }

    @Override
    public Type getOriginType() {
        if (expr != null) {
            return expr.getOriginType();
        }
        return super.getOriginType();
    }

    @Override
    protected String toSqlImpl() {
        return "?";
    }

    @Override
    public int hashCode() {
        return slotId;
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        Parameter parameter = (Parameter) obj;
        return this.slotId == parameter.slotId;
    }
}
