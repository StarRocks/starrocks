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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.type.BooleanType;
import com.starrocks.type.Type;

import java.util.Arrays;
import java.util.List;

/**
 * Bottom-up immutable shuttle that re-derives ScalarOperator types and
 * rebinds CallOperator Function references after a leaf type change.
 *
 * <p>Designed to run over the output of {@code ReplaceColumnRefRewriter}
 * when a leaf substitution crosses type boundaries (e.g. MV rewrite
 * substituting a SMALLINT column with a BIGINT MV column). On any
 * unrecoverable type mismatch, throws {@link TypeReDeriveException}.
 *
 * <p>This visitor has NO MV-specific knowledge. It only knows ScalarOperator
 * type rules. MV-specific concerns (rollup-fn family mapping, candidate
 * rejection plumbing) live in MvColumnRefSubstitutor (added in a later phase).
 *
 * <p>This is the leaves-only skeleton. CallOperator, IF/CaseWhen, Cast, and
 * predicate support are added in subsequent phases.
 */
public final class ScalarOperatorTypeReDeriver
        extends ScalarOperatorVisitor<ScalarOperator, Void> {

    public static ScalarOperator reDerive(ScalarOperator input) {
        return input.accept(new ScalarOperatorTypeReDeriver(), null);
    }

    private ScalarOperatorTypeReDeriver() {}

    @Override
    public ScalarOperator visit(ScalarOperator op, Void ctx) {
        if (op.getChildren().isEmpty()) {
            return op;
        }
        List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(op.getChildren().size());
        boolean changed = false;
        for (ScalarOperator child : op.getChildren()) {
            ScalarOperator nc = child.accept(this, ctx);
            if (nc != child) {
                changed = true;
            }
            newChildren.add(nc);
        }
        if (!changed) {
            return op;
        }
        throw new TypeReDeriveException(
                "ScalarOperatorTypeReDeriver default fallback refuses to re-emit "
                        + op.getClass().getSimpleName()
                        + " after child types changed (op=" + op.debugString() + ")");
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator op, Void ctx) {
        return op;
    }

    @Override
    public ScalarOperator visitConstant(ConstantOperator op, Void ctx) {
        return op;
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, Void ctx) {
        String fnName = call.getFnName();
        List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(call.getChildren().size());
        boolean childChanged = false;
        for (ScalarOperator child : call.getChildren()) {
            ScalarOperator newChild = child.accept(this, ctx);
            newChildren.add(newChild);
            if (newChild != child) {
                childChanged = true;
            }
        }

        Type[] argTypes = newChildren.stream()
                .map(ScalarOperator::getType)
                .toArray(Type[]::new);

        // IF specialization: unify the two value branches before generic resolution.
        if (FunctionSet.IF.equalsIgnoreCase(fnName) && newChildren.size() == 3) {
            Type t1 = newChildren.get(1).getType();
            Type t2 = newChildren.get(2).getType();
            Type unified = TypeManager.getCommonSuperType(t1, t2);
            if (unified == null || !unified.isValid()) {
                throw new TypeReDeriveException(
                        "if branches have no common super type: " + t1 + " vs " + t2);
            }
            ScalarOperator b1 = unified.matchesType(t1)
                    ? newChildren.get(1)
                    : new CastOperator(unified, newChildren.get(1), true);
            ScalarOperator b2 = unified.matchesType(t2)
                    ? newChildren.get(2)
                    : new CastOperator(unified, newChildren.get(2), true);
            Type[] ifArgs = new Type[] {BooleanType.BOOLEAN, unified, unified};
            Function ifFn = resolveFunction(FunctionSet.IF, ifArgs);
            if (ifFn == null) {
                throw new TypeReDeriveException("no IF builtin for arg types " + Arrays.toString(ifArgs));
            }
            CallOperator out = new CallOperator(FunctionSet.IF, unified,
                    Lists.newArrayList(newChildren.get(0), b1, b2), ifFn);
            out.setIgnoreNulls(call.getIgnoreNulls());
            return out;
        }

        // COALESCE specialization: unify all value branches before generic resolution.
        if (FunctionSet.COALESCE.equalsIgnoreCase(fnName) && !newChildren.isEmpty()) {
            Type unified = newChildren.get(0).getType();
            for (int i = 1; i < newChildren.size(); i++) {
                unified = TypeManager.getCommonSuperType(unified, newChildren.get(i).getType());
                if (unified == null || !unified.isValid()) {
                    throw new TypeReDeriveException(
                            "coalesce branches have no common super type: "
                            + newChildren.stream().map(ScalarOperator::getType)
                                    .collect(java.util.stream.Collectors.toList()));
                }
            }
            List<ScalarOperator> aligned = Lists.newArrayListWithCapacity(newChildren.size());
            for (ScalarOperator c : newChildren) {
                aligned.add(unified.matchesType(c.getType()) ? c : new CastOperator(unified, c, true));
            }
            Type[] coalesceArgs = new Type[aligned.size()];
            Arrays.fill(coalesceArgs, unified);
            Function coalesceFn = resolveFunction(FunctionSet.COALESCE, coalesceArgs);
            if (coalesceFn == null) {
                throw new TypeReDeriveException(
                        "no COALESCE builtin for arg types " + Arrays.toString(coalesceArgs));
            }
            CallOperator out = new CallOperator(FunctionSet.COALESCE, unified, aligned, coalesceFn);
            out.setIgnoreNulls(call.getIgnoreNulls());
            return out;
        }

        Function fn = resolveSpecializedAggFn(fnName, argTypes);
        if (fn == null) {
            fn = resolveFunction(fnName, argTypes);
        }
        if (fn == null) {
            throw new TypeReDeriveException(
                    "Cannot re-derive function '" + fnName + "' for arg types " + Arrays.toString(argTypes));
        }

        Function origFn = call.getFunction();
        if (!childChanged && fn == origFn && fn.getReturnType().equals(call.getType())) {
            return call;
        }

        CallOperator newCall = new CallOperator(
                fnName, fn.getReturnType(), newChildren, fn,
                call.isDistinct(), call.isRemovedDistinct());
        newCall.setIgnoreNulls(call.getIgnoreNulls());
        return newCall;
    }

    @Override
    public ScalarOperator visitCastOperator(CastOperator op, Void ctx) {
        ScalarOperator newChild = op.getChild(0).accept(this, ctx);
        if (!op.isImplicit()) {
            // User-written cast: preserve target unconditionally.
            if (newChild == op.getChild(0)) {
                return op;
            }
            return new CastOperator(op.getType(), newChild, false);
        }
        // Implicit cast.
        if (op.getType().matchesType(newChild.getType())) {
            // Cast is now redundant — drop it.
            return newChild;
        }
        if (newChild == op.getChild(0)) {
            return op;
        }
        return new CastOperator(op.getType(), newChild, true);
    }

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator op, Void ctx) {
        ScalarOperator caseClause = op.hasCase() ? op.getCaseClause().accept(this, ctx) : null;
        List<ScalarOperator> whenThen = Lists.newArrayList();
        List<Type> valueTypes = Lists.newArrayList();
        for (int i = 0; i < op.getWhenClauseSize(); i++) {
            ScalarOperator when = op.getWhenClause(i).accept(this, ctx);
            ScalarOperator then = op.getThenClause(i).accept(this, ctx);
            whenThen.add(when);
            whenThen.add(then);
            valueTypes.add(then.getType());
        }
        ScalarOperator elseClause = op.hasElse() ? op.getElseClause().accept(this, ctx) : null;
        if (elseClause != null) {
            valueTypes.add(elseClause.getType());
        }
        Type unified;
        try {
            unified = TypeManager.getCompatibleTypeForCaseWhen(valueTypes);
        } catch (SemanticException e) {
            throw new TypeReDeriveException("case-when branches have no compatible type: " + valueTypes);
        }
        if (unified == null || !unified.isValid()) {
            throw new TypeReDeriveException("case-when branches have no compatible type: " + valueTypes);
        }
        List<ScalarOperator> alignedWhenThen = Lists.newArrayListWithCapacity(whenThen.size());
        for (int i = 0; i < whenThen.size(); i += 2) {
            ScalarOperator then = whenThen.get(i + 1);
            if (!unified.matchesType(then.getType())) {
                then = new CastOperator(unified, then, true);
            }
            alignedWhenThen.add(whenThen.get(i));
            alignedWhenThen.add(then);
        }
        if (elseClause != null && !unified.matchesType(elseClause.getType())) {
            elseClause = new CastOperator(unified, elseClause, true);
        }
        return new CaseWhenOperator(unified, caseClause, elseClause, alignedWhenThen);
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
        ScalarOperator l = op.getChild(0).accept(this, ctx);
        ScalarOperator r = op.getChild(1).accept(this, ctx);
        Type unified = TypeManager.getCommonSuperType(l.getType(), r.getType());
        if (unified == null || !unified.isValid()) {
            throw new TypeReDeriveException(
                    "binary predicate has no common super type: " + l.getType() + " vs " + r.getType());
        }
        if (!unified.matchesType(l.getType())) {
            l = new CastOperator(unified, l, true);
        }
        if (!unified.matchesType(r.getType())) {
            r = new CastOperator(unified, r, true);
        }
        if (l == op.getChild(0) && r == op.getChild(1)) {
            return op;
        }
        return new BinaryPredicateOperator(op.getBinaryType(), l, r);
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator op, Void ctx) {
        List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(op.getChildren().size());
        boolean childChanged = false;
        List<Type> types = Lists.newArrayList();
        for (ScalarOperator child : op.getChildren()) {
            ScalarOperator nc = child.accept(this, ctx);
            if (nc != child) {
                childChanged = true;
            }
            newChildren.add(nc);
            types.add(nc.getType());
        }
        // Compute unified type across all children.
        Type unified = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            unified = TypeManager.getCommonSuperType(unified, types.get(i));
            if (unified == null || !unified.isValid()) {
                throw new TypeReDeriveException("IN has no common type: " + types);
            }
        }
        // Determine whether any child needs casting to the unified type.
        boolean castNeeded = false;
        for (Type t : types) {
            if (!unified.matchesType(t)) {
                castNeeded = true;
                break;
            }
        }
        if (!childChanged && !castNeeded) {
            return op;
        }
        List<ScalarOperator> aligned = Lists.newArrayListWithCapacity(newChildren.size());
        for (ScalarOperator c : newChildren) {
            aligned.add(unified.matchesType(c.getType()) ? c : new CastOperator(unified, c, true));
        }
        return new InPredicateOperator(op.isNotIn(), aligned.toArray(new ScalarOperator[0]));
    }

    private static Function resolveSpecializedAggFn(String name, Type[] argTypes) {
        if (FunctionSet.SUM.equalsIgnoreCase(name) && argTypes.length == 1) {
            return ScalarOperatorUtil.findSumFn(argTypes);
        }
        if (FunctionSet.COUNT.equalsIgnoreCase(name) && argTypes.length == 1) {
            return ExprUtils.getBuiltinFunction(FunctionSet.COUNT, argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        // BITMAP_UNION / HLL_UNION / PERCENTILE_UNION resolve cleanly via the
        // generic path because their builtin signatures take fixed types.
        return null;
    }

    private static Function resolveFunction(String name, Type[] argTypes) {
        Function fn = ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_IDENTICAL);
        if (fn != null) {
            return fn;
        }
        fn = ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn != null) {
            return fn;
        }
        return ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
    }
}
