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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

public class UserVariableExpr extends Expr {

    private final String name;

    private Expr value;

    // True for a variable that the select list of its query block also assigns with ':='. Such a variable changes
    // from row to row and has no value that could be substituted while the statement is analyzed.
    private final boolean assignedPerRow;

    public UserVariableExpr(String name, NodePosition pos) {
        this(name, pos, false);
    }

    public UserVariableExpr(String name, NodePosition pos, boolean assignedPerRow) {
        super(pos);
        this.name = name;
        this.assignedPerRow = assignedPerRow;
    }


    protected UserVariableExpr(UserVariableExpr other) {
        super(other);
        name = other.name;
        value = other.value;
        assignedPerRow = other.assignedPerRow;
    }

    public String getName() {
        return name;
    }

    public Expr getValue() {
        return value;
    }

    public void setValue(Expr value) {
        this.value = value;
        this.type = value.getType();
    }

    public boolean isAssignedPerRow() {
        return assignedPerRow;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUserVariableExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new UserVariableExpr(this);
    }

    @Override
    public boolean equalsWithoutChild(Object o) {
        if (!super.equalsWithoutChild(o)) {
            return false;
        }
        UserVariableExpr that = (UserVariableExpr) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value)
                && assignedPerRow == that.assignedPerRow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, value, assignedPerRow);
    }

    @Override
    public boolean isNullable() {
        if (assignedPerRow) {
            return true;
        }
        Preconditions.checkState(value != null, "should analyze UserVariableExpr first then invoke isNullable");
        return value.isNullable();
    }

    @Override
    protected boolean isConstantImpl() {
        return !assignedPerRow;
    }

}
