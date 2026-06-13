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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/FunctionCallExpr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast.expression;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FunctionCallExpr extends Expr {
    private static final java.util.concurrent.atomic.AtomicLong FN_ID_COUNTER =
            new java.util.concurrent.atomic.AtomicLong(0);

    // Unique ID assigned when the function is resolved. Used as key in AnalysisContext
    // to look up the resolved Function object. Survives clone/copy since it's a plain long.
    private long fnId = -1;

    private FunctionRef fnRef;
    private FunctionParams fnParams;

    // check analytic function
    private boolean isAnalyticFnCall = false;

    // Indicates whether this is a merge aggregation function that should use the merge
    // instead of the update symbol. This flag also affects the behavior of
    // resetAnalysisState() which is used during expr substitution.
    private boolean isMergeAggFn;

    // Cached properties from the resolved Function object, set via AnalysisContext.populateCachedFields().
    private boolean isAggregateFn = false;
    private boolean fnNullable = true;
    private Type[] fnArgTypes;
    private boolean fnHasVarArgs = false;
    private int fnNumArgs = 0;
    private boolean isWindowFunction = false;

    private static final ImmutableSet<String> NON_DETERMINISTIC_FUNCTIONS =
            ImmutableSet.<String>builder()
                    .add("rand").add("random").add("uuid").add("uuid_numeric")
                    .add("uuid_v7").add("uuid_v7_numeric").add("sleep")
                    .build();

    // TODO(yan): add more known functions which are monotonic.
    private static final ImmutableSet<String> MONOTONIC_FUNCTION_SET =
            new ImmutableSet.Builder<String>().add("year").build();

    public boolean isAnalyticFnCall() {
        return isAnalyticFnCall;
    }

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
    }

    public static long nextFnId() {
        return FN_ID_COUNTER.incrementAndGet();
    }

    public long getFnId() {
        return fnId;
    }

    public void setFnId(long fnId) {
        this.fnId = fnId;
    }

    public boolean hasFnId() {
        return fnId >= 0;
    }

    public boolean isAggregateFn() {
        return isAggregateFn;
    }

    public void setAggregateFn(boolean aggregateFn) {
        isAggregateFn = aggregateFn;
    }

    public boolean isFnNullable() {
        return fnNullable;
    }

    public void setFnNullable(boolean fnNullable) {
        this.fnNullable = fnNullable;
    }

    public Type[] getFnArgTypes() {
        return fnArgTypes;
    }

    public void setFnArgTypes(Type[] fnArgTypes) {
        this.fnArgTypes = fnArgTypes;
    }

    public boolean isFnHasVarArgs() {
        return fnHasVarArgs;
    }

    public void setFnHasVarArgs(boolean fnHasVarArgs) {
        this.fnHasVarArgs = fnHasVarArgs;
    }

    public int getFnNumArgs() {
        return fnNumArgs;
    }

    public void setFnNumArgs(int fnNumArgs) {
        this.fnNumArgs = fnNumArgs;
    }

    public boolean isWindowFunction() {
        return isWindowFunction;
    }

    public void setWindowFunction(boolean windowFunction) {
        isWindowFunction = windowFunction;
    }

    public void copyFnFieldsFrom(FunctionCallExpr other) {
        this.fnId = other.fnId;
        this.isAggregateFn = other.isAggregateFn;
        this.fnNullable = other.fnNullable;
        this.fnArgTypes = other.fnArgTypes;
        this.fnHasVarArgs = other.fnHasVarArgs;
        this.fnNumArgs = other.fnNumArgs;
        this.isWindowFunction = other.isWindowFunction;
    }

    public FunctionRef getFnRef() {
        return fnRef;
    }

    // Compatibility method - returns the QualifiedName from FunctionRef
    public QualifiedName getFnName() {
        return fnRef.getFnName();
    }

    public String getFunctionName() {
        return fnRef.getFunctionName();
    }

    public String getDbName() {
        return fnRef.getDbName();
    }

    public void resetFnName(String db, String name) {
        QualifiedName qualifiedName;
        if (db != null) {
            qualifiedName = QualifiedName.of(List.of(db, name));
        } else {
            qualifiedName = QualifiedName.of(List.of(name));
        }
        this.fnRef = new FunctionRef(qualifiedName, null, this.fnRef != null ? this.fnRef.getPos() : NodePosition.ZERO);
    }

    // only used restore from readFields.
    private FunctionCallExpr() {
        super(NodePosition.ZERO);
    }

    public FunctionCallExpr(String functionName, List<Expr> params) {
        this(createFunctionRef(functionName, NodePosition.ZERO), new FunctionParams(false, params), NodePosition.ZERO);
    }

    public FunctionCallExpr(String functionName, List<Expr> params, NodePosition pos) {
        this(createFunctionRef(functionName, pos), new FunctionParams(false, params), pos);
    }

    public FunctionCallExpr(FunctionRef fnRef, List<Expr> params) {
        this(fnRef, new FunctionParams(false, params), NodePosition.ZERO);
    }

    public FunctionCallExpr(FunctionRef fnRef, List<Expr> params, NodePosition pos) {
        this(fnRef, new FunctionParams(false, params), pos);
    }

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(createFunctionRef(fnName, NodePosition.ZERO), params, NodePosition.ZERO);
    }

    public FunctionCallExpr(String fnName, FunctionParams params, NodePosition pos) {
        this(createFunctionRef(fnName, pos), params, pos);
    }

    public FunctionCallExpr(FunctionRef fnRef, FunctionParams params) {
        this(fnRef, params, false, NodePosition.ZERO);
    }

    public FunctionCallExpr(FunctionRef fnRef, FunctionParams params, NodePosition pos) {
        this(fnRef, params, false, pos);
    }

    private FunctionCallExpr(
            FunctionRef fnRef, FunctionParams params, boolean isMergeAggFn, NodePosition pos) {
        super(pos);
        this.fnRef = fnRef;
        fnParams = params;
        this.isMergeAggFn = isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    // Helper method to create FunctionRef from function name string
    private static FunctionRef createFunctionRef(String functionName, NodePosition pos) {
        // Parse db.function or just function
        String[] parts = functionName.split("\\.", 2);
        QualifiedName qualifiedName;
        if (parts.length == 2) {
            qualifiedName = QualifiedName.of(List.of(parts[0], parts[1]), pos);
        } else {
            qualifiedName = QualifiedName.of(List.of(functionName), pos);
        }
        return new FunctionRef(qualifiedName, null, pos);
    }

    // Constructs the same agg function with new params.
    public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
        Preconditions.checkState(e.isAnalyzed);
        Preconditions.checkState(e.isAggregateFunction() || e.isAnalyticFnCall);
        fnRef = e.fnRef;
        isAnalyticFnCall = e.isAnalyticFnCall;
        fnParams = params;
        fnId = e.fnId;
        isAggregateFn = e.isAggregateFn;
        fnNullable = e.fnNullable;
        fnArgTypes = e.fnArgTypes;
        fnHasVarArgs = e.fnHasVarArgs;
        fnNumArgs = e.fnNumArgs;
        isWindowFunction = e.isWindowFunction;
        this.isMergeAggFn = e.isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    protected FunctionCallExpr(FunctionCallExpr other) {
        super(other);
        fnId = other.fnId;
        isAggregateFn = other.isAggregateFn;
        fnNullable = other.fnNullable;
        fnArgTypes = other.fnArgTypes;
        fnHasVarArgs = other.fnHasVarArgs;
        fnNumArgs = other.fnNumArgs;
        isWindowFunction = other.isWindowFunction;
        fnRef = other.fnRef;
        isAnalyticFnCall = other.isAnalyticFnCall;
        // Clone the params in a way that keeps the children_ and the params.exprs()
        // in sync. The children have already been cloned in the super c'tor.
        if (other.fnParams.isStar()) {
            Preconditions.checkState(children.isEmpty());
            fnParams = FunctionParams.createStarParam();
        } else {
            fnParams = new FunctionParams(other.fnParams.isDistinct(), children, other.fnParams.getOrderByElements());
        }
        this.isMergeAggFn = other.isMergeAggFn;
    }

    public static final Set<String> NULLABLE_SAME_WITH_CHILDREN_FUNCTIONS =
            ImmutableSet.<String>builder()
                    .add("year")
                    .add("month")
                    .add("day")
                    .add("hour")
                    .add("add")
                    .add("subtract")
                    .add("multiply")
                    .build();

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    @Override
    public Expr clone() {
        return new FunctionCallExpr(this);
    }

    @Override
    public void resetAnalysisState() {
        isAnalyzed = false;
        // Resolving merge agg functions after substitution may fail e.g., if the
        // intermediate agg type is not the same as the output type. Preserve the original
        // fn_ such that analyze() hits the special-case code for merge agg fns that
        // handles this case.
        if (!isMergeAggFn) {
            isAggregateFn = false;
            fnNullable = true;
            fnArgTypes = null;
            fnHasVarArgs = false;
            fnNumArgs = 0;
            isWindowFunction = false;
        }
    }

    public FunctionParams getFnParams() {
        return fnParams;
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)/*.add("op", aggOp)*/.add("name", fnRef.getFnName().toString()).add("isStar",
                        fnParams.isStar()).add("isDistinct", fnParams.isDistinct()).
                add(" hasOrderBy ", fnParams.getOrderByElements() != null).addValue(
                        super.debugString()).toString();
    }

    @Override
    public String toSimpleSql() {
        StringBuilder sb = new StringBuilder(getFunctionName());
        sb.append("(");
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(children.get(i).toSimpleSql());
        }
        sb.append(")");
        return sb.toString();
    }

    public FunctionParams getParams() {
        return fnParams;
    }

    public boolean isAggregateFunction() {
        return isAggregateFn && !isAnalyticFnCall;
    }

    public boolean isDistinct() {
        return fnParams.isDistinct();
    }

    public boolean hasNullableChild() {
        if (this.isMergeAggFn) {
            return true;
        }

        for (Expr expr : children) {
            if (expr.isNullable()) {
                return true;
            }
        }
        return false;
    }

    // TODO(kks): improve this
    public boolean isNullable() {
        // check if fn always return non null
        if (fnArgTypes != null && !fnNullable) {
            return false;
        }
        // check children nullable
        if (NULLABLE_SAME_WITH_CHILDREN_FUNCTIONS.contains(fnRef.getFunctionName())) {
            return children.stream().anyMatch(e -> e.isNullable() || e.getType().isDecimalV3());
        }
        return true;
    }

    // Used for store load
    public boolean supportSerializable() {
        for (Expr child : children) {
            if (!child.supportSerializable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        // TODO: we can't correctly determine const-ness before analyzing 'fn_'. We should
        // rework logic so that we do not call this function on unanalyzed exprs.
        // Aggregate functions are never constant.
        if (isAggregateFn) {
            return false;
        }

        final String fnName = this.fnRef.getFunctionName();
        // Non-deterministic functions are never constant.
        if (isNondeterministicBuiltinFnName()) {
            return false;
        }
        // Sleep is a special function for testing.
        if (fnName.equalsIgnoreCase("sleep")) {
            return false;
        }
        return super.isConstantImpl();
    }

    /*
        Non-deterministic functions should be mapped multiple times in the project,
        which requires different hashes for each non-deterministic function,
        so in Expression Analyzer, each non-deterministic function will be numbered to achieve different hash values.
    */
    private int nondeterministicId = 0;

    public void setNondeterministicId(int nondeterministicId) {
        this.nondeterministicId = nondeterministicId;
    }

    public boolean isNondeterministicBuiltinFnName() {
        return NON_DETERMINISTIC_FUNCTIONS.contains(fnRef.getFunctionName().toLowerCase());
    }

    @Override
    public int hashCode() {
        // @Note: fnParams is different with children Expr. use children plz.
        return Objects.hash(super.hashCode(), type, fnRef.getFnName().toString(), nondeterministicId);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        FunctionCallExpr o = (FunctionCallExpr) obj;
        return /*opcode == o.opcode && aggOp == o.aggOp &&*/ fnRef.getFnName().toString().equals(o.fnRef.getFnName().toString())
                && fnParams.isDistinct() == o.fnParams.isDistinct()
                && fnParams.isStar() == o.fnParams.isStar()
                && nondeterministicId == o.nondeterministicId
                && Objects.equals(fnParams.getOrderByElements(), o.fnParams.getOrderByElements());
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCall(this, context);
    }

    public void setMergeAggFn() {
        isMergeAggFn = true;
    }

    @Override
    public boolean isSelfMonotonic() {
        FunctionRef ref = getFnRef();
        if (ref.getDbName() == null && MONOTONIC_FUNCTION_SET.contains(ref.getFunctionName())) {
            return true;
        }
        return false;
    }

}
