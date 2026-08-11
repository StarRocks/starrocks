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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SearchDslAstBuilder;
import com.starrocks.sql.parser.SearchDslNode;
import com.starrocks.sql.parser.SearchOptions;

/** Analyzer-stage standard-mode rewriter for the built-in {@code search()} function. */
final class SearchFunctionRewriter {
    private final SearchFunctionValidator.RelationContext relationContext;

    private SearchFunctionRewriter(SearchFunctionValidator.RelationContext relationContext) {
        this.relationContext = relationContext;
    }

    static void rewrite(SelectRelation select) {
        SearchFunctionValidator.RelationContext relationContext =
                SearchFunctionValidator.validateQueryBlock(select);
        if (relationContext == null) {
            return;
        }

        SearchFunctionRewriter rewriter = new SearchFunctionRewriter(relationContext);
        select.setWhereClause(rewriter.rewrite(select.getWhereClause()));
    }

    private Expr rewrite(Expr expression) {
        if (expression instanceof FunctionCallExpr
                && SearchFunctionResolver.isBuiltinSearchInvocation((FunctionCallExpr) expression)) {
            return expand((FunctionCallExpr) expression);
        }
        if (expression instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) expression;
            Expr left = rewrite(compound.getChild(0));
            Expr right = compound.getChildren().size() == 1 ? null : rewrite(compound.getChild(1));
            return new CompoundPredicate(compound.getOp(), left, right, compound.getPos());
        }
        return expression;
    }

    private Expr expand(FunctionCallExpr call) {
        String dsl = ((StringLiteral) call.getChild(0)).getValue();
        SearchOptions options = SearchOptions.defaults();
        if (call.getChildren().size() > 1) {
            try {
                options = SearchOptions.parse(
                        ((StringLiteral) call.getChild(1)).getValue(), call.getChild(1).getPos());
            } catch (ParsingException exception) {
                throw new SemanticException(exception.getDetailMsg(), call.getChild(1).getPos());
            }
        }

        SearchDslNode root;
        try {
            root = SearchDslAstBuilder.parse(dsl, call.getChild(0).getPos());
        } catch (ParsingException exception) {
            throw new SemanticException(exception.getDetailMsg(), call.getChild(0).getPos());
        }
        return new StandardSearchPredicateBuilder(relationContext, options, call.getPos()).build(root);
    }
}
