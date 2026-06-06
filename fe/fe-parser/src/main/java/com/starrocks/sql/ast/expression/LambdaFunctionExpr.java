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

import java.util.List;

public class LambdaFunctionExpr extends Expr {
    private int commonSubOperatorNum = 0;

    // the arguments are lambda expr, lambda arg1, lambda arg2...
    public LambdaFunctionExpr(List<Expr> arguments) {
        this(arguments, NodePosition.ZERO);
    }

    public LambdaFunctionExpr(List<Expr> arguments, NodePosition pos) {
        super(pos);
        this.children.addAll(arguments);
    }

    public LambdaFunctionExpr(List<Expr> arguments, int commonSubOperatorNum) {
        this.children.addAll(arguments);
        this.commonSubOperatorNum = commonSubOperatorNum;
    }

    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
        this.commonSubOperatorNum = rhs.commonSubOperatorNum;
    }

    public int getCommonSubOperatorNum() {
        return commonSubOperatorNum;
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaFunctionExpr(this, context);
    }

}
