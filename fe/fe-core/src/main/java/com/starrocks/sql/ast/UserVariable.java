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
import com.starrocks.sql.parser.NodePosition;

public class UserVariable extends SetListItem {
    private final String variable;
    private Expr unevaluatedExpression;
    private LiteralExpr evaluatedExpression;

    public UserVariable(String variable, Expr unevaluatedExpression) {
        this(variable, unevaluatedExpression, NodePosition.ZERO);
    }

    public UserVariable(String variable, Expr unevaluatedExpression, NodePosition pos) {
        super(pos);
        this.variable = variable;
        this.unevaluatedExpression = unevaluatedExpression;
        if (unevaluatedExpression instanceof LiteralExpr) {
            this.evaluatedExpression = (LiteralExpr) unevaluatedExpression;
        }
    }

    public String getVariable() {
        return variable;
    }

    public Expr getUnevaluatedExpression() {
        return unevaluatedExpression;
    }

    public void setUnevaluatedExpression(Expr unevaluatedExpression) {
        this.unevaluatedExpression = unevaluatedExpression;
    }

    public LiteralExpr getEvaluatedExpression() {
        return evaluatedExpression;
    }

    public void setEvaluatedExpression(LiteralExpr evaluatedExpression) {
        this.evaluatedExpression = evaluatedExpression;
    }
}
