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

package com.starrocks.planner.expression;

import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.ast.expression.CompoundPredicate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility methods for working with {@link ExecExpr} trees.
 * Provides functionality that PlanNode subclasses need (slot collection,
 * tuple binding checks, compound predicate construction, etc.).
 */
public final class ExecExprUtils {

    private ExecExprUtils() {
    }

    /**
     * Check whether the given expression is bound by the specified tuple IDs,
     * i.e., all slot references in the expression refer to slots in one of the given tuples.
     */
    public static boolean isBoundByTupleIds(ExecExpr expr, List<TupleId> tids) {
        if (expr instanceof ExecSlotRef) {
            ExecSlotRef slotRef = (ExecSlotRef) expr;
            // Lambda argument slots may not have a parent tuple; treat them as unbound.
            if (slotRef.getDesc().getParent() == null) {
                return false;
            }
            return tids.contains(slotRef.getTupleId());
        }
        for (ExecExpr child : expr.getChildren()) {
            if (!isBoundByTupleIds(child, tids)) {
                return false;
            }
        }
        // An expression with no slot refs (e.g., a literal) is considered bound by any tuple.
        return true;
    }

    /**
     * Collect all {@link ExecSlotRef} nodes in the expression tree.
     */
    public static List<ExecSlotRef> collectSlotRefs(ExecExpr expr) {
        List<ExecSlotRef> result = new ArrayList<>();
        collectSlotRefsHelper(expr, result);
        return result;
    }

    private static void collectSlotRefsHelper(ExecExpr expr, List<ExecSlotRef> result) {
        if (expr instanceof ExecSlotRef) {
            result.add((ExecSlotRef) expr);
        }
        for (ExecExpr child : expr.getChildren()) {
            collectSlotRefsHelper(child, result);
        }
    }

    /**
     * Get all SlotIds referenced by the expression tree.
     */
    public static Set<SlotId> getUsedSlotIds(ExecExpr expr) {
        Set<SlotId> result = new HashSet<>();
        for (ExecSlotRef slotRef : collectSlotRefs(expr)) {
            result.add(slotRef.getSlotId());
        }
        return result;
    }

    /**
     * Get all SlotIds referenced by a list of expressions.
     */
    public static Set<SlotId> getUsedSlotIds(List<? extends ExecExpr> exprs) {
        Set<SlotId> result = new HashSet<>();
        for (ExecExpr expr : exprs) {
            result.addAll(getUsedSlotIds(expr));
        }
        return result;
    }

    /**
     * Deep-clone a list of expressions.
     */
    @SuppressWarnings("unchecked")
    public static <T extends ExecExpr> List<T> cloneList(List<T> exprs) {
        List<T> result = new ArrayList<>(exprs.size());
        for (T expr : exprs) {
            result.add((T) expr.clone());
        }
        return result;
    }

    /**
     * Build an AND compound predicate from a list of conjuncts.
     * Returns null if the list is empty, the single element if the list has one element,
     * or a left-deep AND tree otherwise.
     */
    public static ExecExpr compoundAnd(List<ExecExpr> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return null;
        }
        ExecExpr result = conjuncts.get(0);
        for (int i = 1; i < conjuncts.size(); i++) {
            result = new ExecCompoundPredicate(CompoundPredicate.Operator.AND, result, conjuncts.get(i));
        }
        return result;
    }

    /**
     * Check whether the given expression tree contains an {@link ExecDictMapping} node.
     */
    public static boolean containsDictMappingExpr(ExecExpr expr) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof ExecDictMapping) {
            return true;
        }
        for (ExecExpr child : expr.getChildren()) {
            if (containsDictMappingExpr(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Unwrap a slot reference from an expression, traversing through CASTs.
     * Returns null if the expression is not a (possibly cast) slot reference.
     */
    public static ExecSlotRef unwrapSlotRef(ExecExpr expr) {
        if (expr instanceof ExecSlotRef) {
            return (ExecSlotRef) expr;
        }
        if (expr instanceof ExecCast && expr.getNumChildren() == 1) {
            return unwrapSlotRef(expr.getChild(0));
        }
        return null;
    }
}
