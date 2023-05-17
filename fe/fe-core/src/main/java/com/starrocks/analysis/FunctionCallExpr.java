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

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TAggregateExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FunctionCallExpr extends Expr {
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

    public void resetFnName(String db, String name) {
        this.fnName = new FunctionName(db, name);
    }

    // only used restore from readFields.
    private FunctionCallExpr() {
        super(NodePosition.ZERO);
    }

    public FunctionCallExpr(String functionName, List<Expr> params) {
        this(new FunctionName(functionName), new FunctionParams(false, params), NodePosition.ZERO);
    }

    public FunctionCallExpr(String functionName, List<Expr> params, NodePosition pos) {
        this(new FunctionName(functionName), new FunctionParams(false, params), pos);
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params) {
        this(fnName, new FunctionParams(false, params), NodePosition.ZERO);
    }

    public FunctionCallExpr(FunctionName fnName, List<Expr> params, NodePosition pos) {
        this(fnName, new FunctionParams(false, params), pos);
    }

    public FunctionCallExpr(String fnName, FunctionParams params) {
        this(new FunctionName(fnName), params, NodePosition.ZERO);
    }

    public FunctionCallExpr(String fnName, FunctionParams params, NodePosition pos) {
        this(new FunctionName(fnName), params, pos);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params) {
        this(fnName, params, false, NodePosition.ZERO);
    }

    public FunctionCallExpr(FunctionName fnName, FunctionParams params, NodePosition pos) {
        this(fnName, params, false, pos);
    }

    private FunctionCallExpr(
            FunctionName fnName, FunctionParams params, boolean isMergeAggFn, NodePosition pos) {
        super(pos);
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
            fnParams = new FunctionParams(other.fnParams.isDistinct(), children, other.fnParams.getOrderByElements());
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
                && nondeterministicId.equals(o.nondeterministicId)
                && Objects.equals(fnParams.getOrderByElements(), o.fnParams.getOrderByElements());
    }

    // TODO: process order by
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
        if (fnParams.getOrderByElements() == null) {
            sb.append(Joiner.on(", ").join(childrenToSql())).append(")");
        } else {
            sb.append(Joiner.on(", ").join(firstNChildrenToSql(
                    children.size() - fnParams.getOrderByElements().size())));
            sb.append(fnParams.getOrderByStringToSql());
            sb.append(')');
        }
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
        if (fnParams.getOrderByElements() == null) {
            sb.append(Joiner.on(", ").join(firstNChildrenToExplain(children.size()))).append(");");
        } else {
            sb.append(Joiner.on(", ").join(firstNChildrenToExplain(
                    children.size() - fnParams.getOrderByElements().size())));
            sb.append(fnParams.getOrderByStringToExplain());
            sb.append(')');
        }
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

    // explain the first N children
    public List<String> firstNChildrenToExplain(int n) {
        Preconditions.checkState(n <= children.size());
        List<String> result = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            result.add(children.get(i).explain());
        }
        return result;
    }

    public List<String> firstNChildrenToSql(int n) {
        Preconditions.checkState(n <= children.size());
        List<String> result = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            result.add(children.get(i).toSql());
        }
        return result;
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)/*.add("op", aggOp)*/.add("name", fnName).add("isStar",
                        fnParams.isStar()).add("isDistinct", fnParams.isDistinct()).
                add(" hasOrderBy ", fnParams.getOrderByElements() != null).addValue(
                        super.debugString()).toString();
    }

    public FunctionParams getParams() {
        return fnParams;
    }

    public boolean isAggregateFunction() {
        Preconditions.checkState(fn != null);
        return fn instanceof AggregateFunction && !isAnalyticFnCall;
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

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
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

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        Type type = getFn().getReturnType();
        if (!type.equals(targetType)) {
            return super.uncheckedCastTo(targetType);
        } else {
            return this;
        }
    }

}
