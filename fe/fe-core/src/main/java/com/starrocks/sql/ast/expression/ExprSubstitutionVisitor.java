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
import com.starrocks.sql.ast.AstVisitorExtendInterface;

/**
 * Visitor-based implementation of expression substitution. It provides a single
 * entry point to rewrite an expression tree based on {@link ExprSubstitutionMap}.
 */
public final class ExprSubstitutionVisitor implements AstVisitorExtendInterface<Expr, ExprSubstitutionMap> {
    private static final ExprSubstitutionVisitor INSTANCE = new ExprSubstitutionVisitor();

    private ExprSubstitutionVisitor() {
    }

    /**
     * Returns a new expression where every occurrence of {@code lhs[i]} is substituted
     * with {@code rhs[i]} from {@code substitutionMap}. The original expression remains unchanged.
     */
    public static Expr rewrite(Expr expr, ExprSubstitutionMap substitutionMap) {
        Preconditions.checkNotNull(expr, "expr cannot be null");
        Preconditions.checkNotNull(substitutionMap, "substitutionMap cannot be null");
        return expr.accept(INSTANCE, substitutionMap);
    }

    @Override
    public Expr visitExpression(Expr node, ExprSubstitutionMap context) {
        Expr replacement = context.get(node);
        if (replacement != null) {
            return replacement.clone();
        }

        Expr cloned = node.clone();
        int childCount = node.getChildren().size();
        for (int i = 0; i < childCount; ++i) {
            Expr rewrittenChild = node.getChild(i).accept(this, context);
            cloned.setChild(i, rewrittenChild);
        }
        return cloned;
    }
}
