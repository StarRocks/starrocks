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

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

/**
 * {@code @name := value}. The expression assigns the value to the user variable and evaluates to it.
 */
public class UserVariableAssignExpr extends Expr {

    private final String name;

    public UserVariableAssignExpr(String name, Expr value, NodePosition pos) {
        super(pos);
        this.name = name;
        addChild(value);
    }

    protected UserVariableAssignExpr(UserVariableAssignExpr other) {
        super(other);
        name = other.name;
    }

    public String getName() {
        return name;
    }

    public Expr getValue() {
        return getChild(0);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUserVariableAssignExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new UserVariableAssignExpr(this);
    }

    @Override
    public boolean equalsWithoutChild(Object o) {
        if (!super.equalsWithoutChild(o)) {
            return false;
        }
        return Objects.equals(name, ((UserVariableAssignExpr) o).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }

    // The variable changes every time the expression is evaluated, so it is never a constant to fold away.
    @Override
    protected boolean isConstantImpl() {
        return false;
    }
}
