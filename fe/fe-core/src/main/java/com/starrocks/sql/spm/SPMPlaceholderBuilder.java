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

package com.starrocks.sql.spm;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.QueryRelation;

import java.util.List;
import java.util.Map;

// replace variables to SPMFunctions
public class SPMPlaceholderBuilder {
    private final PlaceholderBuilder builder = new PlaceholderBuilder();

    private long placeholderID = 0;

    // original expr -> placeholder expr
    private Map<Expr, Expr> generatePlaceholderExprMap = Maps.newHashMap();

    private Map<Expr, Expr> userDefinedPlaceholderMap = Maps.newHashMap();

    public Map<Expr, Expr> getGeneratePlaceholderExprMap() {
        return generatePlaceholderExprMap;
    }

    public Map<Expr, Expr> getUserDefinedPlaceholderMap() {
        return userDefinedPlaceholderMap;
    }

    public QueryRelation build(QueryRelation query) {
        return (QueryRelation) query.accept(builder, null);
    }

    private class PlaceholderBuilder extends SPMUpdateExprVisitor<Expr> {
        @Override
        public ParseNode visitInPredicate(InPredicate node, Expr parent) {
            if (!node.isLiteralChildren()) {
                return super.visitInPredicate(node, parent);
            }
            List<Expr> v = node.getChildren().stream().skip(1).toList();
            Expr spm = SPMFunctions.newFunc(SPMFunctions.CONST_LIST_FUNC, placeholderID++, v);
            Expr in = new InPredicate(node.getChild(0), List.of(spm), node.isNotIn(), node.getPos());
            generatePlaceholderExprMap.put(node, spm);
            return in;
        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Expr parent) {
            Expr s = SPMFunctions.newFunc(SPMFunctions.CONST_VAR_FUNC, placeholderID++, List.of(node));
            generatePlaceholderExprMap.put(parent.clone(), s);
            return s;
        }

        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr parent) {
            if (SPMFunctions.isSPMFunctions(node)) {
                userDefinedPlaceholderMap.put(parent.clone(), node);
                return node;
            }

            return super.visitFunctionCall(node, parent);
        }

        @Override
        public ParseNode visitExpression(Expr node, Expr parent) {
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                for (int i = 0; i < node.getChildren().size(); i++) {
                    node.setChild(i, visitExpr(node.getChild(i), node));
                }
            }
            return node;
        }
    }
}
