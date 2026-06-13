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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.FunctionCallExpr;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores analysis results keyed by fnId, similar to Trino's Analysis pattern.
 * <p>
 * Each FunctionCallExpr gets a unique fnId when resolved. This ID survives
 * clone/copy operations, so cloned exprs can still look up their Function.
 * <p>
 * The global registry allows planner code to retrieve the original Function
 * object without needing to thread an AnalysisContext instance.
 */
public class AnalysisContext {
    // Global fnId → Function registry. Populated by populateCachedFields().
    // fnIds are globally unique (AtomicLong in FunctionCallExpr), so there's no collision risk.
    private static final Map<Long, Function> GLOBAL_FN_REGISTRY = new ConcurrentHashMap<>();

    private final Map<Long, Function> resolvedFunctions = new HashMap<>();

    public void registerFunction(FunctionCallExpr expr, Function fn) {
        long fnId;
        if (expr.hasFnId()) {
            fnId = expr.getFnId();
        } else {
            fnId = FunctionCallExpr.nextFnId();
            expr.setFnId(fnId);
        }
        resolvedFunctions.put(fnId, fn);
        GLOBAL_FN_REGISTRY.put(fnId, fn);
        populateCachedFields(expr, fn);
    }

    /**
     * Populate cached typed fields on FunctionCallExpr from a resolved Function,
     * and register the function in the global registry for later retrieval.
     */
    public static void populateCachedFields(FunctionCallExpr expr, Function fn) {
        if (!expr.hasFnId()) {
            expr.setFnId(FunctionCallExpr.nextFnId());
        }
        GLOBAL_FN_REGISTRY.put(expr.getFnId(), fn);

        expr.setAggregateFn(fn instanceof AggregateFunction);
        expr.setFnNullable(fn.isNullable());
        expr.setFnArgTypes(fn.getArgs());
        expr.setFnHasVarArgs(fn.hasVarArgs());
        expr.setFnNumArgs(fn.getNumArgs());
        expr.setWindowFunction(fn instanceof AggregateFunction && ((AggregateFunction) fn).isAnalyticFn());
    }

    /**
     * Retrieve the resolved Function for a FunctionCallExpr by its fnId.
     * Works across analysis and planner phases.
     */
    public static Function getFunctionByExpr(FunctionCallExpr expr) {
        if (!expr.hasFnId()) {
            return null;
        }
        return GLOBAL_FN_REGISTRY.get(expr.getFnId());
    }

    /**
     * Retrieve a resolved Function by fnId from the global registry.
     */
    public static Function getFunctionById(long fnId) {
        return GLOBAL_FN_REGISTRY.get(fnId);
    }

    public Function getFunction(FunctionCallExpr expr) {
        if (!expr.hasFnId()) {
            return null;
        }
        return resolvedFunctions.get(expr.getFnId());
    }

    public Function getFunction(long fnId) {
        return resolvedFunctions.get(fnId);
    }

    public int registeredCount() {
        return resolvedFunctions.size();
    }

    public String registeredKeys() {
        return resolvedFunctions.keySet().toString();
    }
}
