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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Simple container for expression substitutions, consumed exclusively by
 * {@link ExprSubstitutionVisitor}. Each entry maps an original expression to
 * the expression that should replace it during AST rewrites. Callers must ensure
 * the replacement expr has already been analyzed and is visible in the target
 * scope; otherwise the visitor may generate invalid SlotRefs when cloning.
 */
public final class ExprSubstitutionMap {
    private final List<Expr> lhs;
    private final List<Expr> rhs;

    public ExprSubstitutionMap() {
        this.lhs = Lists.newArrayList();
        this.rhs = Lists.newArrayList();
    }

    /**
     * Adds a mapping from {@code lhsExpr} to {@code rhsExpr}. The visitor will look up entries
     * using {@link Expr#equals(Object)}, so callers should pass the references they expect to replace.
     * The {@code rhsExpr} must already be analyzed and valid in the target scope.
     */
    public void put(Expr lhsExpr, Expr rhsExpr) {
        lhs.add(lhsExpr);
        rhs.add(rhsExpr);
    }

    /**
     * Returns the replacement expression registered for {@code lhsExpr}, or {@code null} if none exists.
     */
    public Expr get(Expr lhsExpr) {
        for (int i = 0; i < lhs.size(); ++i) {
            if (lhsExpr.equals(lhs.get(i))) {
                return rhs.get(i);
            }
        }
        return null;
    }
}
