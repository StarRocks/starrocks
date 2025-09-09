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

/**
 * Show charset statement
 * Acceptable syntax:
 * SHOW {CHAR SET | CHARSET}
 * [LIKE 'pattern' | WHERE expr]
 */
public class ShowCharsetStmt extends ShowStmt {
    private String pattern;
    private Expr where;

    public ShowCharsetStmt() {
        super(NodePosition.ZERO);
    }

    public ShowCharsetStmt(String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.where = where;
    }

    public String getPattern() {
        return pattern;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowCharsetStatement(this, context);
    }
}
