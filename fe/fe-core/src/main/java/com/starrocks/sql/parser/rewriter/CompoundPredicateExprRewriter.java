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

package com.starrocks.sql.parser.rewriter;

import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Optimizes CompoundPredicate expressions by flattening and balancing long chains of
 * AND/OR predicates into a balanced binary tree. This transformation helps avoid
 * deep recursion and potential stack overflow for queries with very long AND/OR chains.
 * The optimization is only applied if the predicate tree exceeds a certain depth threshold.
 *
 * Additionally, optimizes OR predicates where all children are equality predicates
 * on the same column into an IN predicate for better performance.
 */
public class CompoundPredicateExprRewriter {
    /**
     * Rewrites a CompoundPredicate expression into a balanced binary tree if it is a long
     * chain of AND/OR predicates of the same operator and exceeds the flatten threshold.
     * Also rewrites OR chains of equality predicates on the same column into an IN predicate.
     * @param input the input expression
     * @return the rewritten (possibly balanced or IN-optimized) expression
     */
    public Expr rewrite(Expr input) {
        if (Config.compound_predicate_flatten_threshold <= 0 ||
                input == null || !(input instanceof CompoundPredicate)) {
            return input;
        }
        CompoundPredicate compoundPredicate = (CompoundPredicate) input;
        if (compoundPredicate.getDepth() < Config.compound_predicate_flatten_threshold) {
            return input;
        }
        CompoundPredicate.Operator op = compoundPredicate.getOp();
        List<Expr> operands = new ArrayList<>();
        for (Expr child : compoundPredicate.getChildren()) {
            // Flatten left and right subtrees if they are the same operator
            if (child instanceof CompoundPredicate && ((CompoundPredicate) child).getOp() == op) {
                operands.addAll(flattenOperands(child, op));
            } else {
                Expr rewrittenLeft = rewrite(child);
                operands.add(rewrittenLeft);
            }
        }
        // Optimize OR predicates of the form: col = v1 OR col = v2 OR ... => col IN (v1, v2, ...)
        if (op == CompoundPredicate.Operator.OR) {
            InPredicate inPredicate = tryConvertOrToInPredicate(operands);
            if (inPredicate != null) {
                return inPredicate;
            }
        }

        return buildBalanced(operands, op, 0, operands.size() - 1);
    }

    /**
     * Recursively builds a balanced binary tree from a list of operands.
     */
    private Expr buildBalanced(List<Expr> ops, CompoundPredicate.Operator op, int l, int r) {
        if (l == r) {
            return ops.get(l);
        }
        int m = (l + r) >>> 1;
        Expr leftExpr = buildBalanced(ops, op, l, m);
        Expr rightExpr = buildBalanced(ops, op, m + 1, r);
        return new CompoundPredicate(op, leftExpr, rightExpr, NodePosition.ZERO);
    }

    /**
     * Flattens a left- or right-deep tree of CompoundPredicates with the same operator
     * into a list of operands.
     */
    private List<Expr> flattenOperands(Expr expr, CompoundPredicate.Operator op) {
        List<Expr> operands = new ArrayList<>();
        Deque<Expr> stack = new ArrayDeque<>();
        stack.push(expr);
        while (!stack.isEmpty()) {
            Expr current = stack.pop();
            if (current instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) current;
                if (cp.getOp() == op) {
                    for (Expr child : cp.getChildren()) {
                        stack.push(child);
                    }
                    continue;
                }
            }
            operands.add(current);
        }
        return operands;
    }

    /**
     * If all operands are equality predicates (col = value) on the same column,
     * convert to an IN predicate: col IN (v1, v2, ...)
     * Returns null if not convertible.
     */
    private InPredicate tryConvertOrToInPredicate(List<Expr> operands) {
        if (operands.isEmpty()) {
            return null;
        }
        Expr commonPart = null;
        List<Expr> inList = new ArrayList<>();
        NodePosition pos = NodePosition.ZERO;
        for (Expr expr : operands) {
            if (!(expr instanceof BinaryPredicate)) {
                return null;
            }
            BinaryPredicate bp = (BinaryPredicate) expr;
            if (bp.getOp() != BinaryType.EQ) {
                return null;
            }
            Expr left = bp.getChild(0);
            Expr right = bp.getChild(1);
            // Only support SlotRef = LiteralExpr or LiteralExpr = SlotRef
            if (right instanceof LiteralExpr) {
                if (commonPart == null) {
                    commonPart = left;
                } else if (!commonPart.equals(left)) {
                    return null;
                }
                inList.add(right);
                pos = bp.getPos();
            } else if (left instanceof LiteralExpr) {
                if (commonPart == null) {
                    commonPart = right;
                } else if (!commonPart.equals(right)) {
                    return null;
                }
                inList.add(left);
                pos = bp.getPos();
            } else {
                return null;
            }
        }
        if (commonPart == null || inList.isEmpty()) {
            return null;
        }
        return new InPredicate(commonPart, inList, false, pos);
    }
}
