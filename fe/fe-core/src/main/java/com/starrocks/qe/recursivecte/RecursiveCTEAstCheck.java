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
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;

import java.util.Stack;

public class RecursiveCTEAstCheck extends AstTraverser<Boolean, Void> {
    private final ConnectContext context;

    private boolean isBlockRecursiveCte = false;
    private boolean isRecursiveCte = false;
    private boolean isBlockQuery = false;
    private final Stack<String> cteNameStack = new Stack<>();

    private RecursiveCTEAstCheck(ConnectContext context) {
        this.context = context;
    }

    public static RecursiveCTEAstCheck of(ConnectContext context, StatementBase stmt) {
        RecursiveCTEAstCheck checker = new RecursiveCTEAstCheck(context);
        stmt.accept(checker, null);
        return checker;
    }

    public boolean hasBlockRecursiveCte() {
        if (isBlockRecursiveCte && !context.getSessionVariable().isEnableRecursiveCTE()) {
            throw new SemanticException("Recursive CTE with block operation is disabled. " +
                    "Please set 'enable_recursive_cte' to true to enable it.");
        }
        return isBlockRecursiveCte;
    }

    public boolean hasRecursiveCte() {
        return isRecursiveCte;
    }

    @Override
    public Boolean visitCTE(CTERelation node, Void context) {
        if (node.isAnchor()) {
            this.isBlockQuery = false;
            this.cteNameStack.push(node.getName());
            super.visitCTE(node, context);
            this.cteNameStack.pop();
        } else {
            super.visitCTE(node, context);
        }
        return null;
    }

    @Override
    public Boolean visitSubqueryRelation(SubqueryRelation node, Void context) {
        return super.visitSubqueryRelation(node, context);
    }

    @Override
    public Boolean visitSelect(SelectRelation node, Void context) {
        if (node.getGroupBy() != null || node.hasOrderByClause()
                || node.getSelectList().isDistinct() || node.hasAnalyticInfo()) {
            isBlockQuery = true;
        }
        return super.visitSelect(node, context);
    }

    @Override
    public Boolean visitSetOp(SetOperationRelation node, Void context) {
        if (node.getQualifier() != SetQualifier.ALL || !(node instanceof UnionRelation)) {
            isBlockQuery = true;
        }
        return super.visitSetOp(node, context);
    }

    @Override
    public Boolean visitJoin(JoinRelation node, Void context) {
        if (node.getOnPredicate() != null) {
            visit(node.getOnPredicate(), context);
        }
        boolean isLeftRecursive = visit(node.getLeft(), context);
        boolean isRightRecursive = visit(node.getRight(), context);
        if (isRightRecursive) {
            throw new SemanticException("Recursive reference must be in the left side of JOIN");
        }
        if (isLeftRecursive) {
            node.setJoinHint("BROADCAST");
        }
        return isLeftRecursive;
    }

    @Override
    public Boolean visitTable(TableRelation node, Void context) {
        if (cteNameStack.isEmpty()) {
            return false;
        }
        if (node.getName().getDb() == null && node.getName().getTbl().equalsIgnoreCase(cteNameStack.peek())) {
            if (cteNameStack.size() != 1) {
                throw new SemanticException("Doesn't support multi-level recursive CTE");
            }
            isRecursiveCte = true;
            isBlockRecursiveCte |= isBlockQuery;
            return true;
        }
        return false;
    }
}
