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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;

// change one variable.
public class SystemVariable extends SetListItem {
    private final String variable;
    private final SetType type;
    private final Expr unResolvedExpression;
    private LiteralExpr resolvedExpression;

    public SystemVariable(SetType type, String variable, Expr expression) {
        this.type = type;
        this.variable = variable;
        this.unResolvedExpression = expression;
        if (expression instanceof LiteralExpr) {
            this.resolvedExpression = (LiteralExpr) expression;
        }
    }

    public SystemVariable(String variable, Expr unResolvedExpression) {
        this(SetType.SESSION, variable, unResolvedExpression);
    }

    public String getVariable() {
        return variable;
    }

    public SetType getType() {
        return type;
    }

    public Expr getUnResolvedExpression() {
        return unResolvedExpression;
    }

    public void setResolvedExpression(LiteralExpr resolvedExpression) {
        this.resolvedExpression = resolvedExpression;
    }

    public LiteralExpr getResolvedExpression() {
        return resolvedExpression;
    }
}
