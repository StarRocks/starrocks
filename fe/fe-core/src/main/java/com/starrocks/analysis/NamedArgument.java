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

import com.starrocks.sql.ast.AstVisitor;

public class NamedArgument extends Expr {
    private final String name;

    private Expr expr;

    public NamedArgument(String name, Expr expr) {
        this.name = name;
        this.expr = expr;
    }

    public NamedArgument(NamedArgument other) {
        this.name = other.name;
        this.expr = other.expr;
    }

    public String getName() {
        return name;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    @Override
    public Expr clone() {
        return new NamedArgument(name, expr);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNamedArgument(this, context);
    }

    @Override
    protected String toSqlImpl() {
        return name + "=>" + expr.toSql();
    }

    @Override
    public String toString() {
        return name + "=>" + expr.toString();
    }
}
