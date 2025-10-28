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
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeInPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.Subquery;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// replace variables to SPMFunctions
public class SPMPlaceholderBuilder {
    private record PlaceholderExpr(Expr originalExpr, Expr placeholderExpr, Expr parentExpr) {
        public boolean equals(Expr originalExpr, Expr parentExpr) {
            if (parentExpr == null) {
                // root expr
                return this.originalExpr.equals(originalExpr) && this.parentExpr == null;
            } else {
                return this.originalExpr.equals(originalExpr) && parentExpr.equals(this.parentExpr);
            }
        }

        @Override
        public String toString() {
            return "PlaceholderExpr{" +
                    "originalExpr=" + originalExpr.toMySql() +
                    ", placeholderExpr=" + placeholderExpr.toMySql() +
                    ", parentExpr=" + (parentExpr == null ? "null" : parentExpr.toMySql()) +
                    '}';
        }
    }

    private final PlaceholderInserter inserter = new PlaceholderInserter();

    private final PlaceholderFinder finder = new PlaceholderFinder();

    private final Set<Long> userSPMIds = Sets.newHashSet();

    private long placeholderID = 0;

    private final boolean isStrictCheck;

    private final List<PlaceholderExpr> placeholderExprs = Lists.newArrayList();

    public SPMPlaceholderBuilder(boolean isStrictCheck) {
        this.isStrictCheck = isStrictCheck;
    }

    private Optional<Expr> findPlaceholderExpr(Expr expr, Expr parent) {
        List<PlaceholderExpr> l = placeholderExprs.stream()
                .filter(p -> p.equals(expr, parent)).toList();
        return l.size() == 1 ? Optional.of(l.get(0).placeholderExpr.clone()) : Optional.empty();
    }

    // insert placeholder expression to replace expression in query stmt
    public QueryRelation insertPlaceholder(QueryRelation query) {
        return (QueryRelation) query.accept(inserter, null);
    }

    // find placeholder expression from query stmt
    // e.g. user add a query with SPMFunctions manually, we need to find it
    public void findPlaceholder(QueryRelation query) {
        query.accept(finder, null);
    }

    // plan stmt bind placeholder expression, find same
    public void bindPlaceholder(QueryRelation query) {
        PlanASTPlaceholderBinder binder = new PlanASTPlaceholderBinder();
        binder.bind(query);
    }

    private long nextId() {
        do {
            placeholderID++;
        } while (userSPMIds.contains(placeholderID));
        return placeholderID;
    }

    private class PlaceholderFinder extends SPMUpdateExprVisitor<Void> {
        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Void context) {
            if (SPMFunctions.isSPMFunctions(node)) {
                Preconditions.checkState(node.getChild(0).isLiteral());
                long spmId = ((IntLiteral) node.getChild(0)).getValue();
                if (userSPMIds.contains(spmId)) {
                    throw new SemanticException("sql plan found conflict placeholder expression: " + node.toMySql());
                }
                userSPMIds.add(spmId);
                return node;
            }
            return super.visitFunctionCall(node, context);
        }
    }

    private class PlaceholderInserter extends SPMUpdateExprVisitor<Expr> {
        @Override
        public ParseNode visitInPredicate(InPredicate node, Expr root) {
            if (SPMFunctions.isSPMFunctions(node)) {
                return visitExpression(node, root);
            }
            if (!node.isConstantValues()) {
                return visitExpression(node, root);
            }
            node.setChild(0, visitExpr(node.getChild(0), root == null ? node.clone() : root));
            Optional<Expr> placeholder = findPlaceholderExpr(node, root);
            if (placeholder.isPresent()) {
                if (isStrictCheck) {
                    throw new SemanticException("sql plan found conflict placeholder expression: "
                            + (root == null ? node.toMySql() : root.toMySql()));
                } else {
                    return placeholder.get();
                }
            }
            List<Expr> v = node.getChildren().stream().skip(1).collect(Collectors.toList());
            Expr spm = SPMFunctions.newFunc(SPMFunctions.CONST_LIST_FUNC, nextId(), v);
            Expr in = new InPredicate(node.getChild(0), List.of(spm), node.isNotIn(), node.getPos());
            placeholderExprs.add(new PlaceholderExpr(node, in, root));
            return in;
        }

        @Override
        public ParseNode visitLargeInPredicate(LargeInPredicate node, Expr root) {
            return node;
        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Expr root) {
            Optional<Expr> placeholder = findPlaceholderExpr(node, root);
            if (placeholder.isPresent()) {
                if (isStrictCheck) {
                    throw new SemanticException("sql plan found conflict placeholder expression: "
                            + (root == null ? node.toMySql() : root.toMySql()));
                } else {
                    return placeholder.get();
                }
            }
            Expr s = SPMFunctions.newFunc(SPMFunctions.CONST_VAR_FUNC, nextId(), List.of(node));
            PlaceholderExpr p = new PlaceholderExpr(node, s, root);
            placeholderExprs.add(p);
            return s;
        }

        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr root) {
            if (SPMFunctions.isSPMFunctions(node)) {
                PlaceholderExpr p = new PlaceholderExpr(node, node, root);
                placeholderExprs.add(p);
                return node;
            }
            return visitExpression(node, root);
        }

        @Override
        public ParseNode visitCompoundPredicate(CompoundPredicate node, Expr root) {
            // when compound predicate is root, update root
            // e.g. A AND B AND C, set root to be A/B/C
            if (root == null) {
                for (int i = 0; i < node.getChildren().size(); i++) {
                    node.setChild(i, visitExpr(node.getChild(i), null));
                }
            } else {
                // when compound not root, keep root
                // e.g. case when A AND B THEN C ELSE D END, root should be case when
                for (int i = 0; i < node.getChildren().size(); i++) {
                    node.setChild(i, visitExpr(node.getChild(i), root));
                }
            }
            return node;
        }

        @Override
        public ParseNode visitSubqueryExpr(Subquery node, Expr context) {
            node.getQueryStatement().accept(this, null);
            return node;
        }

        @Override
        public ParseNode visitBinaryPredicate(BinaryPredicate node, Expr root) {
            if (node.getChildren().stream().anyMatch(s -> s instanceof Subquery)) {
                // subquery is hard to patten, we ignore handle it, can be supported some easy case
                for (int i = 0; i < node.getChildren().size(); i++) {
                    // must clone root node, because node will update
                    node.setChild(i, visitExpr(node.getChild(i), null));
                }
                return node;
            }
            return visitExpression(node, root);
        }

        @Override
        public ParseNode visitExpression(Expr node, Expr root) {
            if (CollectionUtils.isEmpty(node.getChildren())) {
                return node;
            }
            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                for (int i = 0; i < node.getChildren().size(); i++) {
                    // must clone root node, because node will update
                    node.setChild(i, visitExpr(node.getChild(i), root == null ? node.clone() : root));
                }
            }
            return node;
        }
    }

    private class PlanASTPlaceholderBinder extends PlaceholderInserter {
        private final Set<Expr> usePlaceholders = Sets.newHashSet();

        public void bind(QueryRelation query) {
            query.accept(this, null);
            for (PlaceholderExpr pe : placeholderExprs) {
                if (!usePlaceholders.contains(pe.placeholderExpr)) {
                    throw new SemanticException("can't found expression[" + pe.originalExpr + "] used in plan stmt");
                }
            }
            for (Expr pe : usePlaceholders) {
                if (placeholderExprs.stream().noneMatch(e -> e.placeholderExpr.equals(pe))) {
                    throw new SemanticException("can't found expression[" + pe.toMySql() + "] used in bind stmt");
                }
            }
        }

        @Override
        public ParseNode visitInPredicate(InPredicate node, Expr root) {
            if (SPMFunctions.isSPMFunctions(node)) {
                return visitExpression(node, root);
            }
            if (!node.isConstantValues()) {
                return visitExpression(node, root);
            }

            node.setChild(0, visitExpr(node.getChild(0), root == null ? node.clone() : root));
            Optional<Expr> spm = findPlaceholderExpr(node, root);
            if (spm.isEmpty()) {
                throw new SemanticException("can't find expression placeholder or placeholder conflict, "
                        + "expression : " + node.toMySql());
            }
            usePlaceholders.add(spm.get());
            return spm.get();
        }

        @Override
        public ParseNode visitLargeInPredicate(LargeInPredicate node, Expr root) {
            return node;
        }

        @Override
        public ParseNode visitLiteral(LiteralExpr node, Expr root) {
            Optional<Expr> spm = findPlaceholderExpr(node, root);

            if (spm.isEmpty()) {
                throw new SemanticException("can't find expression placeholder or placeholder conflict, "
                        + "expression : " + node.toMySql());
            }
            usePlaceholders.add(spm.get());
            return spm.get();
        }

        @Override
        public ParseNode visitFunctionCall(FunctionCallExpr node, Expr root) {
            if (SPMFunctions.isSPMFunctions(node)) {
                usePlaceholders.add(node);
                return node;
            }
            return visitExpression(node, root);
        }
    }

}
