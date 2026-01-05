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
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;

import java.util.Stack;

public class RecursiveCTEAstCheck extends AstTraverser<Void, Void> {
    private final Stack<String> cteNameStack = new Stack<>();
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
    public Void visitCTE(CTERelation node, Void context) {
        if (node.isAnchor()) {
            cteNameStack.push(node.getName());
        }
        super.visitCTE(node, context);
        if (node.isAnchor()) {
            cteNameStack.pop();
        }
        return null;
    }

    @Override
    public Void visitTable(TableRelation node, Void context) {
        if (cteNameStack.isEmpty()) {
            return null;
        }
        if (node.getName().getDb() == null && node.getName().getTbl().equalsIgnoreCase(cteNameStack.peek())) {
            if (cteNameStack.size() != 1) {
                throw new SemanticException("Doesn't support multi-level recursive CTE");
            }
            isRecursiveCte = true;
        }
        return null;
    }
}
