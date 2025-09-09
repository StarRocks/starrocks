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

package com.starrocks.sql.formatter;

import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.stream.Collectors;

public class ExprVerboseVisitor extends ExprExplainVisitor {

    @Override
    public String visitFunctionCall(FunctionCallExpr node, Void context) {
        StringBuilder sb = new StringBuilder();

        sb.append(node.getFnName());
        sb.append("[");
        sb.append("(");

        if (node.getFnParams().isStar()) {
            sb.append("*");
        }
        if (node.isDistinct()) {
            sb.append("DISTINCT ");
        }

        if (node.getFnParams().getOrderByElements() == null) {
            sb.append(node.getChildren().stream().map(this::visit).collect(Collectors.joining(", "))).append(");");
        } else {
            sb.append(node.getChildren().stream()
                    .limit(node.getChildren().size() - node.getFnParams().getOrderByElements().size())
                    .map(this::visit).collect(Collectors.joining(", ")));
            sb.append(node.getFnParams().getOrderByStringToExplain());
            sb.append(')');
        }

        if (node.getFn() != null) {
            sb.append(" args: ");
            for (int i = 0; i < node.getFn().getArgs().length; ++i) {
                if (i != 0) {
                    sb.append(',');
                }
                sb.append(node.getFn().getArgs()[i].getPrimitiveType().toString());
            }
            sb.append(";");
            sb.append(" result: ").append(node.getType()).append(";");
        }
        sb.append(" args nullable: ").append(node.hasNullableChild()).append(";");
        sb.append(" result nullable: ").append(node.isNullable());
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String visitCastExpr(CastExpr node, Void context) {
        if (node.isNoOp()) {
            return node.getChild(0).accept(this, context);
        } else {
            return "cast(" + node.getChild(0).accept(this, context) + " as " + node.getType() + ")";
        }
    }

    @Override
    public String visitSlot(SlotRef node, Void context) {
        if (node.getLabel() != null) {
            return "[" + node.getLabel() + "," +
                    " " + node.getDesc().getType() + "," +
                    " " + node.getDesc().getIsNullable() + "]";
        } else {
            return "[" + node.getDesc().getId().asInt() + "," +
                    " " + node.getDesc().getType() + "," +
                    " " + node.getDesc().getIsNullable() + "]";
        }
    }
}
