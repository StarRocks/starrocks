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

package com.starrocks.datacache;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

public class DataCacheExprRewriter {
    private DataCacheExprRewriter() {}

    // mapping, key is column name, value is literal, means column's actual value
    public static Expr rewrite(Expr expr, Map<String, Expr> mapping) {
        Visitor visitor = new Visitor(mapping);
        return bottomUpRewrite(visitor, expr);
    }

    private static Expr bottomUpRewrite(Visitor visitor, Expr expr) {
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr newExpr = bottomUpRewrite(visitor, expr.getChild(i));
            expr.setChild(i, newExpr);
        }
        return visitor.visit(expr);
    }

    private static class Visitor extends AstVisitor<Expr, Void> {
        private final Map<String, Expr> mapping;

        public Visitor(Map<String, Expr> mapping) {
            this.mapping = mapping;
        }

        @Override
        public Expr visitNode(ParseNode node, Void context) {
            return (Expr) node;
        }

        @Override
        public Expr visitSlot(SlotRef node, Void context) {
            Expr value = mapping.get(node.getColumnName());
            if (value != null) {
                return value;
            } else {
                return node;
            }
        }
    }
}
