// This file is made available under Elastic License 2.0.
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

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TAggregateExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class FunctionCallExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(FunctionCallExpr.class);
    private FunctionName fnName;
    // private BuiltinAggregateFunction.Operator aggOp;
    private FunctionParams fnParams;

    // check analytic function
    private boolean isAnalyticFnCall = false;

    // Indicates whether this is a merge aggregation function that should use the merge
    // instead of the update symbol. This flag also affects the behavior of
    // resetAnalysisState() which is used during expr substitution.
    private boolean isMergeAggFn;

    // Indicates merge aggregation function whether has nullable child
    // because when create merge agg fn from update agg fn,
    // The slot SlotDescriptor nullable info will lost or change
    private boolean mergeAggFnHasNullableChild = true;

    private static final ImmutableSet<String> STDDEV_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.STDDEV).add(FunctionSet.STDDEV_VAL).add(FunctionSet.STDDEV_SAMP)
                    .add(FunctionSet.VARIANCE).add(FunctionSet.VARIANCE_POP).add(FunctionSet.VARIANCE_POP)
                    .add(FunctionSet.VAR_SAMP).add(FunctionSet.VAR_POP).build();

    // TODO(yan): add more known functions which are monotonic.
    private static final ImmutableSet<String> MONOTONIC_FUNCTION_SET =
            new ImmutableSet.Builder<String>().add(FunctionSet.YEAR).build();

    public boolean isAnalyticFnCall() {
        return isAnalyticFnCall;
    }

    public void setIsAnalyticFnCall(boolean v) {
        isAnalyticFnCall = v;
    }

    public Function getFn() {
        return fn;
    }

    public FunctionName getFnName() {
        return fnName;
    }

    // only used restore from readFields.
    private FunctionCallExpr() {
        super();
    }

    public FunctionCallExpr(String functionName, List<Expr> params) {
        this(new FunctionName(functionName), new FunctionParams(false, params));
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
        this(fnName, new FunctionParams(false, params));
    }

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(new FunctionName(fnName), params);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
        this(fnName, params, false);
    }

    private FunctionCallExpr(
            FunctionName fnName, FunctionParams params, boolean isMergeAggFn) {
        super();
        this.fnName = fnName;
        fnParams = params;
        this.isMergeAggFn = isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    // Constructs the same agg function with new params.
    public FunctionCallExpr(FunctionCallExpr e, FunctionParams params) {
        Preconditions.checkState(e.isAnalyzed);
        Preconditions.checkState(e.isAggregateFunction() || e.isAnalyticFnCall);
        fnName = e.fnName;
        // aggOp = e.aggOp;
        isAnalyticFnCall = e.isAnalyticFnCall;
        fnParams = params;
        // Just inherit the function object from 'e'.
        fn = e.fn;
        this.isMergeAggFn = e.isMergeAggFn;
        if (params.exprs() != null) {
            children.addAll(params.exprs());
        }
    }

    protected FunctionCallExpr(FunctionCallExpr other) {
        super(other);
        fnName = other.fnName;
        isAnalyticFnCall = other.isAnalyticFnCall;
        //   aggOp = other.aggOp;
        // fnParams = other.fnParams;
        // Clone the params in a way that keeps the children_ and the params.exprs()
        // in sync. The children have already been cloned in the super c'tor.
        if (other.fnParams.isStar()) {
            Preconditions.checkState(children.isEmpty());
            fnParams = FunctionParams.createStarParam();
        } else {
            fnParams = new FunctionParams(other.fnParams.isDistinct(), children);
        }
        this.isMergeAggFn = other.isMergeAggFn;
        this.mergeAggFnHasNullableChild = other.mergeAggFnHasNullableChild;
        fn = other.fn;
    }

    public static final Set<String> nullableSameWithChildrenFunctions =
            ImmutableSet.<String>builder()
                    .add(FunctionSet.YEAR)
                    .add(FunctionSet.MONTH)
                    .add(FunctionSet.DAY)
                    .add(FunctionSet.HOUR)
                    .add(FunctionSet.ADD)
                    .add(FunctionSet.SUBTRACT)
                    .add(FunctionSet.MULTIPLY)
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
            fn = null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        FunctionCallExpr o = (FunctionCallExpr) obj;
        return /*opcode == o.opcode && aggOp == o.aggOp &&*/ fnName.equals(o.fnName)
                && fnParams.isDistinct() == o.fnParams.isDistinct()
                && fnParams.isStar() == o.fnParams.isStar()
                && nondeterministicId.equals(o.nondeterministicId);
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnName);

        sb.append("(");
        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(Joiner.on(", ").join(childrenToSql())).append(")");
        return sb.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnName);

        sb.append("(");
        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("distinct ");
        }
        sb.append(Joiner.on(", ").join(childrenToDigest())).append(")");
        return sb.toString();
    }

    @Override
    public String explainImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnName);

        sb.append("[");
        sb.append("(");
        if (fnParams.isStar()) {
            sb.append("*");
        }
        if (fnParams.isDistinct()) {
            sb.append("distinct ");
        }
        sb.append(Joiner.on(", ").join(childrenToExplain())).append(");");
        if (fn != null) {
            sb.append(" args: ");
            for (int i = 0; i < fn.getArgs().length; ++i) {
                if (i != 0) {
                    sb.append(',');
                }
                sb.append(fn.getArgs()[i].getPrimitiveType().toString());
            }
            sb.append(";");
            sb.append(" result: ").append(type).append(";");
        }
        sb.append(" args nullable: ").append(hasNullableChild()).append(";");
        sb.append(" result nullable: ").append(isNullable());
        sb.append("]");
        return sb.toString();
    }

    public List<String> childrenToExplain() {
        List<String> result = Lists.newArrayList();
        for (Expr child : children) {
            result.add(child.explain());
        }
        return result;
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)/*.add("op", aggOp)*/.add("name", fnName).add("isStar",
                fnParams.isStar()).add("isDistinct", fnParams.isDistinct()).addValue(
                super.debugString()).toString();
    }

    public FunctionParams getParams() {
        return fnParams;
    }

    public boolean isScalarFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof ScalarFunction;
    }

    public boolean isAggregateFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof AggregateFunction && !isAnalyticFnCall;
    }

    public boolean isBuiltin() {
        Preconditions.checkState(fn != null);
        return fn instanceof BuiltinAggregateFunction && !isAnalyticFnCall;
    }

    /**
     * Returns true if this is a call to an aggregate function that returns
     * non-null on an empty input (e.g. count).
     */
    public boolean returnsNonNullOnEmpty() {
        Preconditions.checkNotNull(fn);
        return fn instanceof AggregateFunction
                && ((AggregateFunction) fn).returnsNonNullOnEmpty();
    }

    public boolean isDistinct() {
        Preconditions.checkState(isAggregateFunction());
        return fnParams.isDistinct();
    }

    public boolean isCountStar() {
        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            if (fnParams.isStar()) {
                return true;
            } else if (fnParams.exprs() == null || fnParams.exprs().isEmpty()) {
                return true;
            } else {
                for (Expr expr : fnParams.exprs()) {
                    if (expr.isConstant()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isCountDistinctBitmapOrHLL() {
        if (!fnParams.isDistinct()) {
            return false;
        }

        if (!fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            return false;
        }

        if (children.size() != 1) {
            return false;
        }

        Type type = getChild(0).getType();
        return type.isBitmapType() || type.isHllType();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: we never serialize this to thrift if it's an aggregate function
        // except in test cases that do it explicitly.
        if (isAggregate() || isAnalyticFnCall) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            if (!isAnalyticFnCall) {
                msg.setAgg_expr(new TAggregateExpr(isMergeAggFn));
            }
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        }
    }

    public void analyzeBuiltinAggFunction() throws AnalysisException {
        if (fnParams.isStar() && !fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            throw new AnalysisException(
                    "'*' can only be used in conjunction with COUNT: " + this.toSql());
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            // for multiple exprs count must be qualified with distinct
            if (children.size() > 1 && !fnParams.isDistinct()) {
                throw new AnalysisException(
                        "COUNT must have DISTINCT for multiple arguments: " + this.toSql());
            }

            for (Expr child : children) {
                if (child.type.isPercentile()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.GROUP_CONCAT)) {
            if (children.size() > 2 || children.isEmpty()) {
                throw new AnalysisException(
                        "group_concat requires one or two parameters: " + this.toSql());
            }

            if (fnParams.isDistinct()) {
                throw new AnalysisException("group_concat does not support DISTINCT");
            }

            Expr arg0 = getChild(0);
            if (!arg0.type.isStringType() && !arg0.type.isNull()) {
                throw new AnalysisException(
                        "group_concat requires first parameter to be of type STRING: " + this.toSql());
            }

            if (children.size() == 2) {
                Expr arg1 = getChild(1);
                if (!arg1.type.isStringType() && !arg1.type.isNull()) {
                    throw new AnalysisException(
                            "group_concat requires second parameter to be of type STRING: " + this.toSql());
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.LAG)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.LEAD)) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            } else {
                if (children.size() > 2) {
                    if (!getChild(2).isConstant()) {
                        throw new AnalysisException(
                                "The default parameter (parameter 3) of LAG must be a constant: "
                                        + this.toSql());
                    }
                }
                return;
            }
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.DENSE_RANK)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.RANK)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.ROW_NUMBER)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.FIRST_VALUE)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.LAST_VALUE)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.FIRST_VALUE_REWRITE)) {
            if (!isAnalyticFnCall) {
                throw new AnalysisException(fnName.getFunction() + " only used in analytic function");
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = getChild(0);
        if (arg == null) {
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.ARRAY_AGG)) {
            if (fnParams.isDistinct()) {
                throw new AnalysisException("array_agg does not support DISTINCT");
            }
            if (arg.type.isDecimalV3()) {
                throw new AnalysisException("array_agg does not support DecimalV3");
            }
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.SUM)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.AVG))
                && ((!arg.type.isNumericType() && !arg.type.isNull() && !(arg instanceof NullLiteral)) ||
                !arg.type.canApplyToNumeric())) {
            throw new AnalysisException(fnName.getFunction() + " requires a numeric parameter: " + this.toSql());
        }
        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.SUM_DISTINCT)
                && ((!arg.type.isNumericType() && !arg.type.isNull() && !(arg instanceof NullLiteral)) ||
                !arg.type.canApplyToNumeric())) {
            throw new AnalysisException(
                    "SUM_DISTINCT requires a numeric parameter: " + this.toSql());
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.MIN)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.MAX)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.NDV)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.APPROX_COUNT_DISTINCT))
                && !arg.type.canApplyToNumeric()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_INT) && !arg.type.isIntegerType())) {
            throw new AnalysisException("BITMAP_UNION_INT params only support Integer type");
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.INTERSECT_COUNT)) {
            if (children.size() <= 2) {
                throw new AnalysisException("intersect_count(bitmap_column, column_to_filter, filter_values) " +
                        "function requires at least three parameters");
            }

            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException(
                        "intersect_count function first argument should be of BITMAP type, but was " + inputType);
            }

            if (getChild(1).isConstant()) {
                throw new AnalysisException("intersect_count function filter_values arg must be column");
            }

            for (int i = 2; i < children.size(); i++) {
                if (!getChild(i).isConstant()) {
                    throw new AnalysisException("intersect_count function filter_values arg must be constant");
                }
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_INTERSECT)) {
            if (children.size() != 1) {
                throw new AnalysisException(fnName + " function could only have one child");
            }
            Type inputType = getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new AnalysisException(
                        fnName + " function's argument should be of BITMAP type, but was " + inputType);
            }
            return;
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.HLL_UNION_AGG)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.HLL_UNION)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.HLL_CARDINALITY)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.HLL_RAW_AGG))
                && !arg.type.isHllType()) {
            throw new AnalysisException(
                    "HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.MIN)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.MAX)) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        } else if (fnName.getFunction().equalsIgnoreCase(FunctionSet.DISTINCT_PC)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.DISTINCT_PCSA)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.NDV)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.HLL_UNION_AGG)) {
            fnParams.setIsDistinct(false);
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.PERCENTILE_APPROX)) {
            if (children.size() != 2 && children.size() != 3) {
                throw new AnalysisException("percentile_approx(expr, DOUBLE [, B]) requires two or three parameters");
            }
            if (!getChild(1).isConstant()) {
                throw new AnalysisException("percentile_approx requires second parameter must be a constant : "
                        + this.toSql());
            }
            if (children.size() == 3) {
                if (!getChild(2).isConstant()) {
                    throw new AnalysisException("percentile_approx requires the third parameter must be a constant : "
                            + this.toSql());
                }
            }
        }
    }

    // Provide better error message for some aggregate builtins. These can be
    // a bit more user friendly than a generic function not found.
    // TODO: should we bother to do this? We could also improve the general
    // error messages. For example, listing the alternatives.
    protected String getFunctionNotFoundError(Type[] argTypes) {
        // Some custom error message for builtins
        if (fnParams.isStar()) {
            return "'*' can only be used in conjunction with COUNT";
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            if (!fnParams.isDistinct() && argTypes.length > 1) {
                return "COUNT must have DISTINCT for multiple arguments: " + toSql();
            }
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.SUM)) {
            return "SUM requires a numeric parameter: " + toSql();
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.AVG)) {
            return "AVG requires a numeric or timestamp parameter: " + toSql();
        }

        String[] argTypesSql = new String[argTypes.length];
        for (int i = 0; i < argTypes.length; ++i) {
            argTypesSql[i] = argTypes[i].toSql();
        }

        return String.format(
                "No matching function with signature: %s(%s).",
                fnName, fnParams.isStar() ? "*" : Joiner.on(", ").join(argTypesSql));
    }

    private static final Set<String> DECIMAL_AGG_FUNCTION_SAME_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.MAX).add(FunctionSet.MIN)
                    .add(FunctionSet.LEAD).add(FunctionSet.LAG)
                    .add(FunctionSet.FIRST_VALUE).add(FunctionSet.LAST_VALUE)
                    .add(FunctionSet.ANY_VALUE).build();

    private static final Set<String> DECIMAL_AGG_FUNCTION_WIDER_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.SUM).add(FunctionSet.SUM_DISTINCT).add(FunctionSet.MULTI_DISTINCT_SUM).add("avg")
                    .add(FunctionSet.VARIANCE)
                    .add(FunctionSet.VARIANCE_POP).add(FunctionSet.VAR_POP).add(FunctionSet.VARIANCE_SAMP)
                    .add(FunctionSet.VAR_SAMP)
                    .add(FunctionSet.STDDEV).add(FunctionSet.STDDEV_POP).add(FunctionSet.STD)
                    .add(FunctionSet.STDDEV_SAMP).build();

    private static final Set<String> DECIMAL_AGG_VARIANCE_STDDEV_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.VARIANCE).add(FunctionSet.VARIANCE_POP).add(FunctionSet.VAR_POP)
                    .add(FunctionSet.VARIANCE_SAMP).add(FunctionSet.VAR_SAMP)
                    .add(FunctionSet.STDDEV).add(FunctionSet.STDDEV_POP).add(FunctionSet.STD)
                    .add(FunctionSet.STDDEV_SAMP).build();

    private static final Set<String> DECIMAL_AGG_FUNCTION =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .addAll(DECIMAL_AGG_FUNCTION_SAME_TYPE)
                    .addAll(DECIMAL_AGG_FUNCTION_WIDER_TYPE).build();

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (isMergeAggFn) {
            // This is the function call expr after splitting up to a merge aggregation.
            // The function has already been analyzed so just do the minimal sanity
            // check here.
            AggregateFunction aggFn = (AggregateFunction) fn;
            Preconditions.checkNotNull(aggFn);
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.COUNT) && fnParams.isDistinct()) {
            // Treat COUNT(DISTINCT ...) special because of how we do the equal.
            // There is no version of COUNT() that takes more than 1 argument but after
            // the equal, we only need count(*).
            // TODO: fix how we equal count distinct.
            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[0],
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            type = fn.getReturnType();

            // Make sure BE doesn't see any TYPE_NULL exprs
            for (int i = 0; i < children.size(); ++i) {
                if (getChild(i).getType().isNull()) {
                    uncheckedCastChild(Type.BOOLEAN, i);
                }
            }
            return;
        }
        Type[] argTypes = new Type[this.children.size()];
        for (int i = 0; i < this.children.size(); ++i) {
            this.children.get(i).analyze(analyzer);
            argTypes[i] = this.children.get(i).getType();
        }

        Type decimalReturnType = DecimalV3FunctionAnalyzer.normalizeDecimalArgTypes(argTypes, fnName.getFunction());
        analyzeBuiltinAggFunction();

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.SUM)) {
            if (this.children.isEmpty()) {
                throw new AnalysisException("The " + fnName + " function must has one input param");
            }
            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[] {getChild(0).type},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.getFunction().equalsIgnoreCase("count_distinct")) {
            Type compatibleType = this.children.get(0).getType();
            for (int i = 1; i < this.children.size(); ++i) {
                Type type = this.children.get(i).getType();
                compatibleType = Type.getAssignmentCompatibleType(compatibleType, type, true);
                if (compatibleType.isInvalid()) {
                    compatibleType = Type.VARCHAR;
                    break;
                }
            }

            fn = getBuiltinFunction(analyzer, fnName.getFunction(), new Type[] {compatibleType},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else {
            // now first find function in built-in functions
            if (Strings.isNullOrEmpty(fnName.getDb())) {
                fn = getBuiltinFunction(analyzer, fnName.getFunction(), argTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }

            // find user defined functions
            if (fn == null) {
                if (!analyzer.isUDFAllowed()) {
                    throw new AnalysisException(
                            "Does not support non-builtin functions, or function does not exist: " + this.toSqlImpl());
                }

                String dbName = fnName.analyzeDb(analyzer);
                if (!Strings.isNullOrEmpty(dbName)) {
                    // check operation privilege
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(
                            ConnectContext.get(), dbName, PrivPredicate.SELECT)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
                    }
                    Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
                    if (db != null) {
                        Function searchDesc = new Function(
                                fnName, collectChildReturnTypes(), Type.INVALID, false);
                        fn = db.getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    }
                }
            }
        }

        if (fn == null) {
            LOG.warn("fn {} not exists", this.toSqlImpl());
            throw new AnalysisException(getFunctionNotFoundError(collectChildReturnTypes()));
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            if (children.size() != 2) {
                throw new AnalysisException("date_trunc function must have 2 arguments");
            }

            if (children.get(1).getType().isDatetime()) {
                final StringLiteral fmtLiteral = (StringLiteral) children.get(0);
                if (!fmtLiteral.getStringValue().equals("second") &&
                        !fmtLiteral.getStringValue().equals("minute") &&
                        !fmtLiteral.getStringValue().equals("hour") &&
                        !fmtLiteral.getStringValue().equals("day") &&
                        !fmtLiteral.getStringValue().equals("month") &&
                        !fmtLiteral.getStringValue().equals("year") &&
                        !fmtLiteral.getStringValue().equals("week") &&
                        !fmtLiteral.getStringValue().equals("quarter")) {
                    throw new AnalysisException("date_trunc function can't support argument other than " +
                            "second|minute|hour|day|month|year|week|quarter");
                }
            } else if (children.get(1).getType().isDate()) {
                final StringLiteral fmtLiteral = (StringLiteral) children.get(0);
                if (!fmtLiteral.getStringValue().equals("day") &&
                        !fmtLiteral.getStringValue().equals("month") &&
                        !fmtLiteral.getStringValue().equals("year") &&
                        !fmtLiteral.getStringValue().equals("week") &&
                        !fmtLiteral.getStringValue().equals("quarter")) {
                    throw new AnalysisException("date_trunc function can't support argument other than " +
                            "day|month|year|week|quarter");
                }
            }
        }

        if (fn.getFunctionName().getFunction().equals("time_diff")) {
            fn.getReturnType().getPrimitiveType().setTimeType();
            return;
        }

        if (isAggregateFunction()) {
            final String functionName = fnName.getFunction();
            // subexprs must not contain aggregates
            if (Expr.containsAggregate(children)) {
                throw new AnalysisException(
                        "aggregate function cannot contain aggregate parameters: " + this.toSql());
            }

            if (STDDEV_FUNCTION_SET.contains(functionName) && argTypes[0].isDateType()) {
                throw new AnalysisException("Stddev/variance function do not support Date/Datetime type");
            }

            if (functionName.equalsIgnoreCase(FunctionSet.MULTI_DISTINCT_SUM) && argTypes[0].isDateType()) {
                throw new AnalysisException("Sum in multi distinct functions do not support Date/Datetime type");
            }

            if (children.stream().anyMatch(child -> child.getType().matchesType(Type.TIME))) {
                throw new AnalysisException("Time Type can not used in " + functionName + " function");
            }
        } else {
            if (fnParams.isStar()) {
                throw new AnalysisException("Cannot pass '*' to scalar function.");
            }
            if (fnParams.isDistinct()) {
                throw new AnalysisException("Cannot pass 'DISTINCT' to scalar function.");
            }
        }

        Type[] args = fn.getArgs();
        if (args.length > 0) {
            // Implicitly cast all the children to match the function if necessary
            for (int i = 0; i < argTypes.length; ++i) {
                // For varargs, we must compare with the last type in callArgs.argTypes.
                int ix = Math.min(args.length - 1, i);
                if (args[ix].isDecimalV3() && !argTypes[i].matchesType(getChild(i).getType())) {
                    uncheckedCastChild(argTypes[i], i);
                    continue;
                }
                if (!argTypes[i].matchesType(args[ix]) && (fn.isVectorized() || !(
                        argTypes[i].isDateType() && args[ix].isDateType()))) {
                    uncheckedCastChild(args[ix], i);
                }
            }
        }
        if (DECIMAL_AGG_FUNCTION.contains(fnName.getFunction())
                && Arrays.stream(fn.getArgs()).anyMatch(t -> t.isWildcardDecimal() && t.isDecimalV3())) {
            Type argType = argTypes[0];
            // stddev/variance always use decimal128(38,9) to computing result.
            if (DECIMAL_AGG_VARIANCE_STDDEV_TYPE.contains(fnName.getFunction()) && argType.isDecimalV3()) {
                argType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
                castChild(argType, 0);
            }
            this.fn = DecimalV3FunctionAnalyzer
                    .rectifyAggregationFunction((AggregateFunction) this.fn, argType, decimalReturnType);
            this.type = fn.getReturnType();
        } else if ((fn.getReturnType().isDecimalV3() && decimalReturnType.isValid())) {
            this.type = decimalReturnType;
        } else {
            this.type = fn.getReturnType();
        }
    }

    public void setMergeAggFnHasNullableChild(boolean value) {
        this.mergeAggFnHasNullableChild = value;
    }

    public boolean hasNullableChild() {
        if (this.isMergeAggFn) {
            return this.mergeAggFnHasNullableChild;
        }

        // For BE code simply, handle the following window functions with nullable
        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.LEAD) ||
                fnName.getFunction().equalsIgnoreCase(FunctionSet.LAG) ||
                fnName.getFunction().equalsIgnoreCase(FunctionSet.FIRST_VALUE) ||
                fnName.getFunction().equalsIgnoreCase(FunctionSet.LAST_VALUE)) {
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
        if (fn != null && !fn.isNullable()) {
            return false;
        }
        // check children nullable
        if (nullableSameWithChildrenFunctions.contains(fnName.getFunction())) {
            return children.stream().anyMatch(e -> e.isNullable() || e.getType().isDecimalV3());
        }
        return true;
    }

    public static FunctionCallExpr createMergeAggCall(
            FunctionCallExpr agg, List<Expr> params) {
        Preconditions.checkState(agg.isAnalyzed);
        Preconditions.checkState(agg.isAggregateFunction());
        FunctionCallExpr result = new FunctionCallExpr(
                agg.fnName, new FunctionParams(false, params), true);
        // Inherit the function object from 'agg'.
        result.fn = agg.fn;
        result.type = agg.type;
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fnName.write(out);
        fnParams.write(out);
        out.writeBoolean(isAnalyticFnCall);
        out.writeBoolean(isMergeAggFn);
    }

    public void readFields(DataInput in) throws IOException {
        fnName = FunctionName.read(in);
        fnParams = FunctionParams.read(in);
        if (fnParams.exprs() != null) {
            children.addAll(fnParams.exprs());
        }
        isAnalyticFnCall = in.readBoolean();
        isMergeAggFn = in.readBoolean();
    }

    public static FunctionCallExpr read(DataInput in) throws IOException {
        FunctionCallExpr func = new FunctionCallExpr();
        func.readFields(in);
        return func;
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
        if (fn instanceof AggregateFunction) {
            return false;
        }

        final String fnName = this.fnName.getFunction();
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
    private ExprId nondeterministicId = new ExprId(0);

    public void setNondeterministicId(ExprId nondeterministicId) {
        this.nondeterministicId = nondeterministicId;
    }

    public boolean isNondeterministicBuiltinFnName() {
        return FunctionSet.nonDeterministicFunctions.contains(fnName.getFunction().toLowerCase());
    }

    @Override
    public int hashCode() {
        // fnParams contains all information of children Expr. No need to calculate super's hashcode again.
        return Objects.hash(type, opcode, fnName, fnParams, nondeterministicId);
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
        FunctionName name = getFnName();
        if (name.getDb() == null && MONOTONIC_FUNCTION_SET.contains(name.getFunction())) {
            return true;
        }
        return false;
    }
}
