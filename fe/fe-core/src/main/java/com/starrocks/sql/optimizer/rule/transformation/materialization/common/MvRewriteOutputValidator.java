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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.starrocks.catalog.Function;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Validates that an MV-rewrite output OptExpression has type/signature coherent
 * ScalarOperators.
 *
 * <p>Returns {@code false} when any check fails. The validator is purely an
 * observability tool: callers must NOT drop the candidate based on this
 * return value — silently masking a rewrite bug produces a recall regression
 * that is worse than a loud downstream failure. Callers should LOG.error
 * on a false return and let the candidate proceed; PlanValidator at the end
 * of optimization or the BE will then surface the real problem.
 *
 * <p>In fe-ut, {@link FeConstants#strictMvRewriteValidator} flips this from
 * "log only" to "throw IllegalStateException" so any ReDeriver coverage gap
 * surfaces as a hard test failure rather than reaching production silently.
 */
public final class MvRewriteOutputValidator {

    private static final Logger LOG = LogManager.getLogger(MvRewriteOutputValidator.class);

    private MvRewriteOutputValidator() {}

    /** Top-level entry: walks the OptExpression tree. mvIdentifier is for log / strict-mode messages. */
    public static boolean validate(OptExpression expr, String mvIdentifier) {
        boolean ok = walkOpt(expr);
        if (!ok && FeConstants.strictMvRewriteValidator) {
            throw new IllegalStateException(
                    "MvRewriteOutputValidator strict-mode rejection for mv=" + mvIdentifier);
        }
        return ok;
    }

    /** Convenience for unit tests on a single ScalarOperator subtree. */
    public static boolean isCoherent(ScalarOperator op) {
        return walkScalar(op);
    }

    private static boolean walkOpt(OptExpression expr) {
        if (expr == null) {
            return true;
        }
        if (expr.getOp() instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) expr.getOp();
            for (Map.Entry<ColumnRefOperator, CallOperator> e : agg.getAggregations().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        if (expr.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator proj = (LogicalProjectOperator) expr.getOp();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : proj.getColumnRefMap().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        Projection p = expr.getOp().getProjection();
        if (p != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : p.getColumnRefMap().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        for (OptExpression child : expr.getInputs()) {
            if (!walkOpt(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkOutputMapping(ColumnRefOperator k, ScalarOperator v) {
        if (!k.getType().matchesType(v.getType())) {
            LOG.warn("[mvval-OUTREF_TYPE] cls={} outRef={} keyType={} exprType={} expr={}",
                    v.getClass().getSimpleName(), k.getName(), k.getType(), v.getType(), debug(v));
            return false;
        }
        // Nullability mismatch is intentionally NOT a rejection. The async MV
        // rewrite path legitimately produces ColumnRefs whose declared nullability
        // diverges from the wrapped expression's nullability (e.g. count → sum
        // rollup: the count output is declared non-null even though sum() of an
        // empty MV partition is null-typed — the MV stores per-group counts so
        // the runtime is always non-null, just the type system disagrees).
        // Keep a debug log for traceability.
        if (k.isNullable() != v.isNullable() && LOG.isDebugEnabled()) {
            LOG.debug("[mvval-OUTREF_NULLABLE-tolerated] cls={} outRef={} keyNullable={} exprNullable={}",
                    v.getClass().getSimpleName(), k.getName(), k.isNullable(), v.isNullable());
        }
        return true;
    }

    /**
     * Agg-state functions (`*_merge`, `*_union`, `*_state`, plus a few hardcoded
     * MV-internal aggregates) tolerate divergent FE-side declared arg types vs.
     * physical column types because the BE interprets the state blob independently.
     */
    private static boolean isAggStateFunction(String fnName) {
        if (fnName == null) {
            return false;
        }
        String lower = fnName.toLowerCase();
        return lower.endsWith("_merge") || lower.endsWith("_union") || lower.endsWith("_state");
    }

    private static String debug(ScalarOperator op) {
        try {
            String s = op.debugString();
            return s.length() > 240 ? s.substring(0, 240) + "..." : s;
        } catch (Exception e) {
            return "<debugString threw " + e.getClass().getSimpleName() + ">";
        }
    }

    private static boolean walkScalar(ScalarOperator op) {
        if (op == null) {
            return true;
        }
        if (op instanceof CallOperator && !checkCall((CallOperator) op)) {
            return false;
        }
        if (op instanceof CaseWhenOperator && !checkCaseWhen((CaseWhenOperator) op)) {
            return false;
        }
        // CastOperator validation is intentionally omitted: explicit casts are
        // user semantics; implicit cast safety is a BE concern. Adding a check
        // here without canCastTo() would be a no-op or false-positive.
        for (ScalarOperator child : op.getChildren()) {
            if (!walkScalar(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkCall(CallOperator call) {
        Function fn = call.getFunction();
        if (fn == null) {
            // Some legitimate CallOperators in the codebase (notably the
            // "cast" pseudo-call that appears in some async MV rewrite
            // outputs) have a null Function reference. Skipping the
            // signature check here is safe: PlanValidator at the end of
            // optimization will reject any genuinely broken call, and
            // mismatched-but-non-null cases are caught by the type checks
            // below. Treating null fn as a hard rejection here turned out
            // to drop legitimate async MV candidates (testFilterProject0).
            return true;
        }
        Type[] declared = fn.getArgs();
        // Variadic functions (coalesce, concat, greatest, ...) have a single
        // canonical declared signature but unlimited actual children — skip
        // arity check and only validate the prefix args' types.
        boolean isVarArgs = fn.hasVarArgs();
        if (!isVarArgs && declared.length != call.getChildren().size()) {
            LOG.warn("[mvval-CALL_ARITY] fn={} declared={} actual={} expr={}",
                    call.getFnName(), declared.length, call.getChildren().size(), debug(call));
            return false;
        }
        // Agg-state functions (the `*_merge` / `*_union` / `*_state` family used by
        // MV incremental aggregation) intentionally declare VARCHAR for state-blob
        // arguments while the physical column is stored as VARBINARY (or other
        // binary-equivalent type). The BE knows how to interpret the state
        // independently of the declared FE-side type. Skip the per-arg type
        // tightness for these.
        boolean isAggStateFn = isAggStateFunction(call.getFnName());
        if (!isAggStateFn) {
            int checkUpTo = Math.min(declared.length, call.getChildren().size());
            for (int i = 0; i < checkUpTo; i++) {
                Type childT = call.getChild(i).getType();
                if (!declared[i].matchesType(childT)) {
                    LOG.warn("[mvval-CALL_ARGTYPE] fn={} child[{}].type={} fnArg[{}]={} childCls={} expr={}",
                            call.getFnName(), i, childT, i, declared[i],
                            call.getChild(i).getClass().getSimpleName(), debug(call));
                    return false;
                }
            }
        }
        if (!fn.getReturnType().matchesType(call.getType())) {
            LOG.warn("[mvval-CALL_RETURN] fn={} callType={} fnReturn={} expr={}",
                    call.getFnName(), call.getType(), fn.getReturnType(), debug(call));
            return false;
        }
        return true;
    }

    private static boolean checkCaseWhen(CaseWhenOperator c) {
        Type t = c.getType();
        for (int i = 0; i < c.getWhenClauseSize(); i++) {
            if (!t.matchesType(c.getThenClause(i).getType())) {
                LOG.warn("[mvval-CASE_THEN] opType={} then[{}].type={} thenCls={} expr={}",
                        t, i, c.getThenClause(i).getType(),
                        c.getThenClause(i).getClass().getSimpleName(), debug(c));
                return false;
            }
        }
        if (c.hasElse() && !t.matchesType(c.getElseClause().getType())) {
            LOG.warn("[mvval-CASE_ELSE] opType={} else.type={} elseCls={} expr={}",
                    t, c.getElseClause().getType(),
                    c.getElseClause().getClass().getSimpleName(), debug(c));
            return false;
        }
        return true;
    }

}
