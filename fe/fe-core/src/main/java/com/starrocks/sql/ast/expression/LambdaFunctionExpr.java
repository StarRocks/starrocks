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
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

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

    // transformed from operators before pushing down to BE.
    public LambdaFunctionExpr(List<Expr> arguments, Map<SlotRef, Expr> commonSubOperatorMap) {
        this.children.addAll(arguments);
        this.commonSubOperatorNum = commonSubOperatorMap.size();
        if (!commonSubOperatorMap.isEmpty()) { // flatten commonSubOperatorMap's slot_id and sub_expr to children
            children.addAll(commonSubOperatorMap.keySet());
            children.addAll(commonSubOperatorMap.values());
        }
    }


    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
        this.commonSubOperatorNum = rhs.commonSubOperatorNum;
    }

    public int getCommonSubOperatorNum() {
        return commonSubOperatorNum;
    }

    public void checkValidAfterToExpr() { // before to operator, its type is LambdaArgument, after to SlotRef
        // 1 lambda expr, at least 1 lambda argument and common sub op's slotref + expr
        Preconditions.checkState(children.size() >= 2 + 2 * commonSubOperatorNum,
                "lambda expr's children num " + children.size() + " should >= " + (2 + 2 * commonSubOperatorNum));
        int realChildrenNum = getChildren().size() - 2 * commonSubOperatorNum;
        for (int i = 1; i < realChildrenNum; i++) {
            Preconditions.checkState(getChild(i) instanceof SlotRef,
                    i + "-th lambda argument should be type of SlotRef, but actually " + getChild(i));
        }
    }


    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitLambdaFunctionExpr(this, context);
    }

}
