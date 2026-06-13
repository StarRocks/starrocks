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
package com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.PercentileCompression;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.FloatType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.Type;

import java.util.Arrays;
import java.util.OptionalDouble;

import static com.starrocks.catalog.Function.CompareMode.IS_IDENTICAL;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_APPROX;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_APPROX_RAW;
import static com.starrocks.catalog.FunctionSet.PERCENTILE_UNION;

// Rewrite percentile_approx using MV's percentile_union(percentile_hash(...)).
// Compression-aware: if MV digest's compression < query's, the rewrite is
// non-subsume (silently downgrades precision). Signals via EquivalentShuttleContext;
// flag flows into MvRewriteContext and BestMvSelector.CandidateScore which ranks
// subsume rewrites first. Session var enable_mv_percentile_strict_match turns
// the warning into a hard skip (fail reason logged via OptimizerTraceUtil).
public class PercentileRewriteEquivalent extends IAggregateRewriteEquivalent {
    public static IAggregateRewriteEquivalent INSTANCE = new PercentileRewriteEquivalent();
    private static final long LEGACY_STORAGE = 1000;

    public PercentileRewriteEquivalent() {
    }

    @Override
    public RewriteEquivalentContext prepare(ScalarOperator op) {
        if (op == null || !(op instanceof CallOperator)) {
            return null;
        }
        CallOperator aggFunc = (CallOperator) op;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equals(PERCENTILE_UNION)) {
            ScalarOperator arg0 = aggFunc.getChild(0);
            if (arg0 == null) {
                return null;
            }
            if (!arg0.getType().isPercentile()) {
                return null;
            }
            if (arg0 instanceof CallOperator) {
                CallOperator call0 = (CallOperator) arg0;
                if (call0.getFnName().equals(FunctionSet.PERCENTILE_HASH)) {
                    // percentile_union(percentile_hash()) can be used for rewrite
                    return new RewriteEquivalentContext(call0.getChild(0), op);
                }
            } else {
                return new RewriteEquivalentContext(arg0, op);
            }
        }
        return null;
    }

    @Override
    public boolean isSupportPushDownRewrite(CallOperator aggFunc) {
        if (aggFunc == null) {
            return false;
        }

        String aggFuncName = aggFunc.getFnName();
        if (aggFuncName.equalsIgnoreCase(PERCENTILE_APPROX)) {
            return true;
        }
        return false;
    }

    @Override
    public ScalarOperator rewrite(RewriteEquivalentContext eqContext,
                                  EquivalentShuttleContext shuttleContext,
                                  ColumnRefOperator replace,
                                  ScalarOperator newInput) {
        if (newInput == null || !(newInput instanceof CallOperator)) {
            return null;
        }
        ScalarOperator eqChild = eqContext.getEquivalent();
        CallOperator aggFunc = (CallOperator) newInput;
        String aggFuncName = aggFunc.getFnName();

        if (aggFuncName.equalsIgnoreCase(PERCENTILE_APPROX)) {
            ScalarOperator eqArg = aggFunc.getChild(0);
            if (!eqArg.equals(eqChild)) {
                return null;
            }
            // Asymmetric check: only mvC < queryC is a problem (precision loss).
            // mvC >= queryC means the MV digest is at least as precise as asked.
            double queryC = extractQueryCompression(aggFunc);
            double mvC = extractMvCompression(eqContext.getInput());
            if (mvC < queryC) {
                // Carry the flag + both values on the shuttle so the per-MV
                // MvRewriteContext / BestMvSelector can prefer subsume MVs, and
                // strict-mode trace can quote the actual compressions.
                shuttleContext.setPercentileNonSubsumeRewrite(true);
                shuttleContext.setPercentileMismatchMvC(mvC);
                shuttleContext.setPercentileMismatchQueryC(queryC);
                if (isStrictMatchEnabled()) {
                    // Strict: refuse this MV; caller falls back to base scan.
                    return null;
                }
                // Legacy: still rewrite but downstream picks subsume if available.
            }
            return rewriteImpl(shuttleContext, aggFunc, replace);
        }
        return null;
    }

    public static boolean isStrictMatchEnabled() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return false;
        }
        SessionVariable sv = ctx.getSessionVariable();
        return sv != null && sv.isEnableMvPercentileStrictMatch();
    }

    private static double extractQueryCompression(CallOperator percentileApprox) {
        // percentile_approx(v, q[, c]) — compression is the third arg if present.
        if (percentileApprox.getChildren().size() > 2) {
            Double c = readDoubleConstant(percentileApprox.getChild(2));
            if (c != null) {
                return c;
            }
        }
        // Query without an explicit compression is interpreted as exact DEFAULT
        // (mirrors BE's PercentileApproxAggregateFunction::get_compression_factor).
        return PercentileCompression.DEFAULT;
    }

    // Walks the MV-side ScalarOperator tree captured in prepare() and reads the
    // stored compression. Accepts every shape prepare() admits and never
    // blind-casts. Anything that does not look like percentile_hash(col[, c]) is
    // treated as legacy storage with compression=1000.
    private static double extractMvCompression(ScalarOperator input) {
        if (!(input instanceof CallOperator)) {
            return LEGACY_STORAGE;
        }
        CallOperator unionCall = (CallOperator) input;
        if (unionCall.getChildren().isEmpty()) {
            return LEGACY_STORAGE;
        }
        ScalarOperator child0 = unionCall.getChild(0);
        if (!(child0 instanceof CallOperator)) {
            // Legacy MV shape: percentile_union(percentile_col) with a direct
            // column-ref child. Pre-`percentile_hash(_, c)` data was written
            // with the default TDigest(1000), hence LEGACY_STORAGE.
            return LEGACY_STORAGE;
        }
        CallOperator hashCall = (CallOperator) child0;
        if (!FunctionSet.PERCENTILE_HASH.equalsIgnoreCase(hashCall.getFnName())) {
            // Defensive: PercentileRewriteEquivalent.prepare() only registers
            // percentile_union(percentile_hash(...)) or percentile_union(non-call),
            // so a CallOperator with a different name should never reach here.
            // Fall back to LEGACY_STORAGE rather than crash if prepare() is
            // ever broadened.
            return LEGACY_STORAGE;
        }
        if (hashCall.getChildren().size() > 1) {
            Double c = readDoubleConstant(hashCall.getChild(1));
            if (c != null) {
                return c;
            }
        }
        return LEGACY_STORAGE;
    }

    // FunctionAnalyzer canonicalizes compression to an integer literal; implicit
    // int→double casts are folded before this matcher runs, so the tree always
    // carries a plain ConstantOperator here. Finite-value post-filter is an
    // extra invariant.
    private static Double readDoubleConstant(ScalarOperator op) {
        if (!(op instanceof ConstantOperator)) {
            return null;
        }
        ConstantOperator c = (ConstantOperator) op;
        if (c.isNull()) {
            return null;
        }
        OptionalDouble v = ConstantOperatorUtils.doubleValueFromConstant(c);
        if (!v.isPresent()) {
            return null;
        }
        double d = v.getAsDouble();
        return Double.isFinite(d) ? d : null;
    }

    private CallOperator makePercentileUnion(ScalarOperator replace) {
        Function unionFn = ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_UNION, new Type[] { PercentileType.PERCENTILE },
                IS_IDENTICAL);
        Preconditions.checkState(unionFn != null);
        return new CallOperator(PERCENTILE_UNION, PercentileType.PERCENTILE, Arrays.asList(replace), unionFn);
    }

    private CallOperator makePercentileApproxRaw(ScalarOperator replace, ScalarOperator arg1) {
        Function approxRawFn = ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_APPROX_RAW,
                new Type[] { PercentileType.PERCENTILE, FloatType.DOUBLE }, Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkState(approxRawFn != null);
        // percentile_approx_raw(percentile_union(input), arg1)
        return new CallOperator(PERCENTILE_APPROX_RAW, FloatType.DOUBLE, Arrays.asList(replace, arg1), approxRawFn);
    }

    private CallOperator makeRollupFunc(ScalarOperator replace, ScalarOperator arg1) {
        Function unionFn = ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_UNION, new Type[] { PercentileType.PERCENTILE },
                IS_IDENTICAL);
        Preconditions.checkState(unionFn != null);
        CallOperator rollup = new CallOperator(PERCENTILE_UNION, PercentileType.PERCENTILE, Arrays.asList(replace), unionFn);

        Function approxRawFn = ExprUtils.getBuiltinFunction(FunctionSet.PERCENTILE_APPROX_RAW,
                new Type[] { PercentileType.PERCENTILE, FloatType.DOUBLE }, Function.CompareMode.IS_IDENTICAL);
        Preconditions.checkState(approxRawFn != null);
        // percentile_approx_raw(percentile_union(input), arg1)
        return new CallOperator(PERCENTILE_APPROX_RAW, FloatType.DOUBLE, Arrays.asList(rollup, arg1), approxRawFn);
    }

    @Override
    public ScalarOperator rewriteRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                     CallOperator aggFunc,
                                                     ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        return makeRollupFunc(replace, arg1);
    }

    @Override
    public ScalarOperator rewriteAggregateFuncWithoutRollup(EquivalentShuttleContext shuttleContext,
                                                            CallOperator aggFunc,
                                                            ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        return makePercentileApproxRaw(replace, arg1);
    }

    @Override
    public Pair<CallOperator, CallOperator> rewritePushDownRollupAggregateFunc(EquivalentShuttleContext shuttleContext,
                                                                               CallOperator aggFunc,
                                                                               ColumnRefOperator replace) {
        ScalarOperator arg1 = aggFunc.getChild(1);
        CallOperator finalFn = makeRollupFunc(replace, arg1);
        CallOperator partialFn = makePercentileUnion(replace);
        return Pair.create(partialFn, finalFn);
    }
}
