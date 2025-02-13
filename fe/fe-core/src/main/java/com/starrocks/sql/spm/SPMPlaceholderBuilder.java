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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;

import java.util.List;
import java.util.Optional;
import java.util.Set;

// replace variables to SPMFunctions
public class SPMPlaceholderBuilder {
    private static class PlaceholderExpr {
        final Expr originalExpr;
        final Expr placeholderExpr;
        final Expr parentExpr;

        public PlaceholderExpr(Expr originalExpr, Expr placeholderExpr, Expr parentExpr) {
            this.originalExpr = originalExpr;
            this.placeholderExpr = placeholderExpr;
            this.parentExpr = parentExpr;
        }

        public boolean equals(Expr originalExpr, Expr parentExpr) {
            if (parentExpr == null) {
                // root expr
                return this.originalExpr.equals(originalExpr) && this.parentExpr == null;
            } else {
                return this.originalExpr.equals(originalExpr) && this.parentExpr.equals(parentExpr);
            }
        }
    }

    private final PlaceholderBuilder builder = new PlaceholderBuilder();

    private final PlaceholderFinder finder = new PlaceholderFinder();

    private final Set<Long> userDefineIds = Sets.newHashSet();

    private long placeholderID = 0;

    private final List<PlaceholderExpr> placeholderExprs = Lists.newArrayList();

    public Set<Long> getUserDefineIds() {
        return userDefineIds;
    }

    public Optional<Expr> findPlaceholderExpr(Expr expr, Expr parent) {
        List<PlaceholderExpr> l = placeholderExprs.stream().filter(p -> p.equals(expr, parent)).toList();
        return l.size() == 1 ? Optional.of(l.get(0).placeholderExpr) : Optional.empty();
    }

    public QueryRelation insertPlaceholder(QueryRelation query) {
        return (QueryRelation) query.accept(builder, null);
    }

    public boolean findPlaceholder(QueryRelation query) {
        query.accept(finder, null);
        return !userDefineIds.isEmpty();
    }

    private long nextId() {
        do {
            placeholderID++;
        } while (userDefineIds.contains(placeholderID));
        return placeholderID;
    }

    private class PlaceholderFinder extends SPMUpdateExprVisitor<Expr> {
        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr parent) {
            if (SPMFunctions.isSPMFunctions(node)) {
                Preconditions.checkState(node.getChild(0).isLiteral());
                long spmId = ((IntLiteral) node.getChild(0)).getValue();
                if (userDefineIds.contains(spmId)) {
                    throw new SemanticException("sql plan found conflict placeholder expression: " + node.toMySql());
                }
                userDefineIds.add(spmId);
                return node;
            }
            return super.visitFunctionCall(node, parent);
        }
    }

    private class PlaceholderBuilder extends SPMUpdateExprVisitor<Expr> {
        @Override
        public ParseNode visitInPredicate(InPredicate node, Expr parent) {
            if (!node.isLiteralChildren()) {
                return super.visitInPredicate(node, parent);
            }
            if (placeholderExprs.stream().anyMatch(p -> p.equals(node, parent))) {
                throw new SemanticException("sql plan found conflict placeholder expression: " + node.toMySql());
            }
            List<Expr> v = node.getChildren().stream().skip(1).toList();
            Expr spm = SPMFunctions.newFunc(SPMFunctions.CONST_LIST_FUNC, nextId(), v);
            Expr in = new InPredicate(node.getChild(0), List.of(spm), node.isNotIn(), node.getPos());
            placeholderExprs.add(new PlaceholderExpr(node, in, node));
            return in;
        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Expr parent) {
            Expr s = SPMFunctions.newFunc(SPMFunctions.CONST_VAR_FUNC, nextId(), List.of(node));
            if (placeholderExprs.stream().anyMatch(p -> p.equals(node, parent))) {
                throw new SemanticException("sql plan found conflict placeholder expression: " + node.toMySql());
            }
            PlaceholderExpr p = new PlaceholderExpr(node, s, parent.clone());
            placeholderExprs.add(p);
            return s;
        }

        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr context) {
            if (SPMFunctions.isSPMFunctions(node)) {
                return node;
            }
            return super.visitFunctionCall(node, context);
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
