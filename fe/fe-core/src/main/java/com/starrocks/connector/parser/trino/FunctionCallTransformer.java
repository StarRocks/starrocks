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

package com.starrocks.connector.parser.trino;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * FunctionCallTransformer is used to convert a Trino function to an StarRocks function, we use the
 * {@link PlaceholderExpr} to represent the input arguments of the function, these PlaceholderExpr
 * will be replaced by actual arguments using {@link FunctionCallRewriter}.
 * Before transform, it need to use match to check whether the FunctionCallTransformer can work.
 */
public class FunctionCallTransformer {
    private final String fnName;
    private final FunctionCallExpr targetCall;
    private final PlaceholderExpr[] placeholderExprs;
    private final int argNums;

    public FunctionCallTransformer(FunctionCallExpr targetCall, int argNums) {
        this.fnName = targetCall.getFnName().getFunction();
        this.targetCall = targetCall;
        this.argNums = argNums;
        this.placeholderExprs = new PlaceholderExpr[argNums];
        init();
    }

    private void init() {
        // collect the all placeholderExprs which defined by targetCall
        new PlaceholderCollector(placeholderExprs).visit(targetCall);
    }

    private static class PlaceholderCollector extends AstVisitor<Void, Void> {
        private final PlaceholderExpr[] placeholderExprs;
        public PlaceholderCollector(PlaceholderExpr[] placeholderExprs) {
            this.placeholderExprs = placeholderExprs;
        }

        @Override
        public Void visitExpression(Expr node, Void context) {
            for (Expr child : node.getChildren()) {
                visit(child);
            }
            return null;
        }

        @Override
        public Void visitPlaceholderExpr(PlaceholderExpr node, Void context) {
            placeholderExprs[node.getIndex() - 1] = node;
            return null;
        }
    }

    public boolean match(List<Expr> sourceArguments) {
        List<Class<? extends Expr>> argTypes = sourceArguments.stream().map(Expr::getClass).collect(Collectors.toList());
        if (sourceArguments.size() != argNums) {
            return false;
        }
        for (int index = 0; index < argNums; ++index) {
            if (!placeholderExprs[index].getClazz().isAssignableFrom(argTypes.get(index))) {
                return false;
            }
        }
        return true;
    }

    public Expr transform(List<Expr> sourceArguments) {
        return new FunctionCallRewriter(placeholderExprs, sourceArguments).visit(targetCall);
    }

    private static class FunctionCallRewriter extends AstVisitor<Expr, Void> {
        private final List<Expr> sourceArguments;

        public FunctionCallRewriter(PlaceholderExpr[] placeholderExprs, List<Expr> sourceArguments) {
            Preconditions.checkState(placeholderExprs.length == sourceArguments.size());
            this.sourceArguments = sourceArguments;
        }

        @Override
        public Expr visitExpression(Expr node, Void context) {
            for (int index = 0; index < node.getChildren().size(); ++index) {
                node.setChild(index, visit(node.getChild(index)));
            }
            return node;
        }

        @Override
        public Expr visitPlaceholderExpr(PlaceholderExpr node, Void context) {
            return this.sourceArguments.get(node.getIndex() - 1);
        }
    }
}
