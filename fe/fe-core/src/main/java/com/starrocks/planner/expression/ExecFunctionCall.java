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

import com.starrocks.catalog.Function;
import com.starrocks.thrift.TAggregateExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunction;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan function call (scalar, aggregate, or analytic).
 */
public class ExecFunctionCall extends ExecExpr {
    private final Function fn;
    private final String fnName;
    private final boolean isDistinct;
    private final boolean ignoreNulls;
    private final boolean isAggregateOrAnalytic;
    private final boolean isCountStar;
    private boolean isMergeAggFn;
    private boolean nullable;

    public ExecFunctionCall(Type type, Function fn, String fnName,
                            List<ExecExpr> children,
                            boolean isDistinct, boolean ignoreNulls,
                            boolean isAggregateOrAnalytic, boolean isMergeAggFn) {
        this(type, fn, fnName, children, isDistinct, ignoreNulls, isAggregateOrAnalytic, isMergeAggFn, false);
    }

    public ExecFunctionCall(Type type, Function fn, String fnName,
                            List<ExecExpr> children,
                            boolean isDistinct, boolean ignoreNulls,
                            boolean isAggregateOrAnalytic, boolean isMergeAggFn,
                            boolean isCountStar) {
        super(type, children);
        this.fn = fn;
        this.fnName = fnName;
        this.isDistinct = isDistinct;
        this.ignoreNulls = ignoreNulls;
        this.isAggregateOrAnalytic = isAggregateOrAnalytic;
        this.isMergeAggFn = isMergeAggFn;
        this.isCountStar = isCountStar;
        this.nullable = true;
    }

    private ExecFunctionCall(ExecFunctionCall other) {
        super(other.type, other.cloneChildren());
        this.fn = other.fn;
        this.fnName = other.fnName;
        this.isCountStar = other.isCountStar;
        this.isDistinct = other.isDistinct;
        this.ignoreNulls = other.ignoreNulls;
        this.isAggregateOrAnalytic = other.isAggregateOrAnalytic;
        this.isMergeAggFn = other.isMergeAggFn;
        this.nullable = other.nullable;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public Function getFn() {
        return fn;
    }

    public String getFnName() {
        return fnName;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isIgnoreNulls() {
        return ignoreNulls;
    }

    public boolean isAggregateOrAnalytic() {
        return isAggregateOrAnalytic;
    }

    public boolean isCountStar() {
        return isCountStar;
    }

    public boolean isMergeAggFn() {
        return isMergeAggFn;
    }

    public void setMergeAggFn() {
        this.isMergeAggFn = true;
    }

    /**
     * Creates a new ExecFunctionCall that replaces the function name, function definition,
     * and distinct flag while keeping the same children and type.
     */
    public ExecFunctionCall withReplacedFunction(String newFnName, Function newFn, boolean newIsDistinct) {
        ExecFunctionCall result = new ExecFunctionCall(
                this.type, newFn, newFnName, this.cloneChildren(),
                newIsDistinct, this.ignoreNulls, this.isAggregateOrAnalytic, this.isMergeAggFn);
        result.nullable = this.nullable;
        return result;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean hasNullableChild() {
        if (isMergeAggFn) {
            return true;
        }
        return super.hasNullableChild();
    }

    @Override
    public TExprNodeType getNodeType() {
        if (isAggregateOrAnalytic) {
            return TExprNodeType.AGG_EXPR;
        }
        return TExprNodeType.FUNCTION_CALL;
    }

    @Override
    public void toThrift(TExprNode node) {
        if (isAggregateOrAnalytic) {
            node.setAgg_expr(new TAggregateExpr(isMergeAggFn));
        }
        if (fn != null) {
            TFunction tfn = fn.toThrift();
            tfn.setIgnore_nulls(ignoreNulls);
            node.setFn(tfn);
            if (fn.hasVarArgs()) {
                node.setVararg_start_idx(fn.getNumArgs() - 1);
            }
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecFunctionCall(this, context);
    }

    @Override
    public ExecFunctionCall clone() {
        return new ExecFunctionCall(this);
    }
}
