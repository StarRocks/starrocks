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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE;
import static java.util.Objects.requireNonNull;

/**
 * Scalar operator support function call
 */
public class CallOperator extends ScalarOperator {
    private final String fnName;
    /**
     * TODO:
     * We need a FunctionHandle to store the required information
     * to determine a unique function signature
     */
    //private final FunctionSignature signature;

    protected List<ScalarOperator> arguments;

    private final Function fn;
    // The flag for distinct function
    private final boolean isDistinct;

    // Ignore nulls.
    private boolean ignoreNulls = false;

    private List<String> hints = Collections.emptyList();
    public CallOperator(String fnName, Type returnType, List<ScalarOperator> arguments) {
        this(fnName, returnType, arguments, null);
    }

    public CallOperator(String fnName, Type returnType, List<ScalarOperator> arguments, Function fn) {
        this(fnName, returnType, arguments, fn, false);
    }

    public CallOperator(String fnName, Type returnType, List<ScalarOperator> arguments, Function fn,
                        boolean isDistinct) {
        super(OperatorType.CALL, returnType);
        this.fnName = requireNonNull(fnName, "fnName is null");
        this.arguments = new ArrayList<>(requireNonNull(arguments, "arguments is null"));
        this.fn = fn;
        this.isDistinct = isDistinct;
    }

    public void setIgnoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    public boolean getIgnoreNulls() {
        return ignoreNulls;
    }

    public String getFnName() {
        return fnName;
    }

    public Function getFunction() {
        return fn;
    }

    public List<ScalarOperator> getArguments() {
        return arguments;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountStar() {
        return fnName.equalsIgnoreCase(FunctionSet.COUNT) && arguments.isEmpty();
    }

    public boolean isAggregate() {
        return fn != null && fn instanceof AggregateFunction;
    }

    @Override
    public String toString() {
        return fnName + "(" + (isDistinct ? "distinct " : "") +
                arguments.stream().map(ScalarOperator::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String debugString() {
        if (fnName.equalsIgnoreCase(FunctionSet.ADD)) {
            return getChild(0).debugString() + " + " + getChild(1).debugString();
        } else if (fnName.equalsIgnoreCase(FunctionSet.SUBTRACT)) {
            return getChild(0).debugString() + " - " + getChild(1).debugString();
        } else if (fnName.equalsIgnoreCase(FunctionSet.MULTIPLY)) {
            return getChild(0).debugString() + " * " + getChild(1).debugString();
        } else if (fnName.equalsIgnoreCase(FunctionSet.DIVIDE)) {
            return getChild(0).debugString() + " / " + getChild(1).debugString();
        }

        return fnName + "(" + arguments.stream().map(ScalarOperator::debugString).collect(Collectors.joining(", ")) +
                ")";
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(index);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(index, child);
    }

    @Override
    public boolean isNullable() {
        // check if fn always return non null
        if (fn != null && !fn.isNullable()) {
            return false;
        }
        // check children nullable
        if (FunctionCallExpr.nullableSameWithChildrenFunctions.contains(fnName)) {
            // decimal operation may overflow
            return arguments.stream()
                    .anyMatch(argument -> argument.isNullable() || argument.getType().isDecimalOfAnyVersion());
        }
        return true;
    }

    public ColumnRefSet getUsedColumns() {
        ColumnRefSet used = new ColumnRefSet();
        for (ScalarOperator child : arguments) {
            used.union(child.getUsedColumns());
        }
        return used;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fnName, arguments, isDistinct);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CallOperator other = (CallOperator) obj;

        return isDistinct == other.isDistinct &&
                Objects.equals(fnName, other.fnName) &&
                Objects.equals(type, other.type) &&
                Objects.equals(arguments, other.arguments);
    }

    public void setHints(List<String> hints) {
        this.hints = hints;
    }

    public List<String> getHints() {
        return hints;
    }

    @Override
    public ScalarOperator clone() {
        CallOperator operator = (CallOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        operator.hints = Lists.newArrayList(hints);
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCall(this, context);
    }

    public static CallOperator buildMultiCountDistinct(CallOperator oldFunctionCall) {
        Function searchDesc = new Function(new FunctionName(FunctionSet.MULTI_DISTINCT_COUNT),
                oldFunctionCall.getFunction().getArgs(), Type.INVALID, false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fn.getReturnType(), oldFunctionCall.getChildren(),
                        fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildSum(ColumnRefOperator arg) {
        Preconditions.checkArgument(arg.getType() == Type.BIGINT);
        Function searchDesc = new Function(new FunctionName(FunctionSet.SUM),
                new Type[] {arg.getType()}, arg.getType(), false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(FunctionSet.SUM, fn.getReturnType(), Lists.newArrayList(arg), fn),
                DEFAULT_TYPE_CAST_RULE);
    }

    public static CallOperator buildMultiSumDistinct(CallOperator oldFunctionCall) {
        Function multiDistinctSum = DecimalV3FunctionAnalyzer.convertSumToMultiDistinctSum(
                oldFunctionCall.getFunction(), oldFunctionCall.getChild(0).getType());
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        return (CallOperator) scalarOpRewriter.rewrite(
                new CallOperator(
                        FunctionSet.MULTI_DISTINCT_SUM, multiDistinctSum.getReturnType(),
                        oldFunctionCall.getChildren(), multiDistinctSum), DEFAULT_TYPE_CAST_RULE);
    }
}
