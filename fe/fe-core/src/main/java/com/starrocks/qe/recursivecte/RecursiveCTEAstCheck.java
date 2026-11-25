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

package com.starrocks.qe.recursivecte;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * Detects recursive CTEs before analysis, following the same scoping rules as QueryAnalyzer:
 * a CTE is a recursive candidate only if its WITH clause is WITH RECURSIVE and its definition is a UNION.
 * Only a recursive candidate is visible inside its own recursive members (not its anchor); an ordinary CTE
 * enters the scope after its definition, and an inner CTE shadows an outer CTE of the same name.
 * A recursive reference made while another recursive candidate is being defined (including inside its
 * anchor) is a multi-level recursive CTE and is rejected.
 */
public class RecursiveCTEAstCheck extends AstTraverser<Void, Void> {
    private static final class CteScopeEntry {
        private final String name;
        // true while the recursive members of a recursive candidate are being traversed
        private final boolean definingRecursive;

        private CteScopeEntry(String name, boolean definingRecursive) {
            this.name = name;
            this.definingRecursive = definingRecursive;
        }
    }

    private final List<CteScopeEntry> cteScope = new ArrayList<>();
    // whether the WITH clause currently being traversed is WITH RECURSIVE
    private boolean inRecursiveWithClause = false;
    // number of recursive candidates whose definition (anchor included) is being traversed
    private int recursiveDefinitionDepth = 0;
    private boolean isRecursiveCte = false;

    public static boolean hasRecursiveCte(StatementBase stmt, ConnectContext context) {
        if (!context.getSessionVariable().isEnableRecursiveCTE()) {
            return false;
        }
        RecursiveCTEAstCheck check = new RecursiveCTEAstCheck();
        stmt.accept(check, null);
        return check.isRecursiveCte;
    }

    @Override
    public Void visitSelect(SelectRelation node, Void context) {
        return visitWithScope(node, () -> super.visitSelect(node, context));
    }

    @Override
    public Void visitSetOp(SetOperationRelation node, Void context) {
        return visitWithScope(node, () -> super.visitSetOp(node, context));
    }

    @Override
    public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
        return visitWithScope(node, () -> {
            // For WITH x AS (...) (SELECT ...), the CTEs belong to the SubqueryRelation itself
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(x -> visit(x, context));
            }
            super.visitSubqueryRelation(node, context);
        });
    }

    private Void visitWithScope(QueryRelation node, Runnable visitor) {
        int scopeSize = cteScope.size();
        boolean savedInRecursiveWithClause = inRecursiveWithClause;
        inRecursiveWithClause = node.isHasRecursiveCTE();
        try {
            visitor.run();
        } finally {
            inRecursiveWithClause = savedInRecursiveWithClause;
            // the CTEs defined by this query are visible only inside it
            cteScope.subList(scopeSize, cteScope.size()).clear();
        }
        return null;
    }

    @Override
    public Void visitCTE(CTERelation node, Void context) {
        if (inRecursiveWithClause && node.getCteQueryStatement().getQueryRelation() instanceof UnionRelation union) {
            visitRecursiveCandidate(node.getName(), union, context);
        } else {
            super.visitCTE(node, context);
        }
        cteScope.add(new CteScopeEntry(node.getName(), false));
        return null;
    }

    // Like QueryAnalyzer.tryProcessRecursiveCte: the anchor (first UNION member) is traversed before the
    // CTE itself is in scope, and the other members can reference it recursively.
    private void visitRecursiveCandidate(String name, UnionRelation union, Void context) {
        recursiveDefinitionDepth++;
        try {
            visitWithScope(union, () -> {
                if (union.hasWithClause()) {
                    union.getCteRelations().forEach(x -> visit(x, context));
                }
                List<QueryRelation> members = union.getRelations();
                visit(members.get(0), context);
                cteScope.add(new CteScopeEntry(name, true));
                members.subList(1, members.size()).forEach(x -> visit(x, context));
            });
        } finally {
            recursiveDefinitionDepth--;
        }
    }

    @Override
    public Void visitTable(TableRelation node, Void context) {
        if (node.getName().getDb() != null) {
            return null;
        }
        String tableName = node.getName().getTbl();
        for (int i = cteScope.size() - 1; i >= 0; i--) {
            CteScopeEntry entry = cteScope.get(i);
            if (!entry.name.equals(tableName)) {
                continue;
            }
            if (entry.definingRecursive) {
                // a self reference from inside a recursive CTE definition
                if (recursiveDefinitionDepth != 1) {
                    throw new SemanticException("Doesn't support multi-level recursive CTE");
                }
                isRecursiveCte = true;
            }
            return null;
        }
        return null;
    }
}
