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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

// Show variables statement.
public class ShowVariablesStmt extends ShowStmt {
    private SetType type;
    private final String pattern;
    private Expr where;

    public ShowVariablesStmt(SetType type, String pattern) {
        this(type, pattern, null, NodePosition.ZERO);
    }

    public ShowVariablesStmt(SetType type, String pattern, Expr where) {
        this(type, pattern, where, NodePosition.ZERO);
    }

    public ShowVariablesStmt(SetType type, String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public String getPattern() {
        return pattern;
    }

    public Expr getWhereClause() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowVariablesStatement(this, context);
    }
}
