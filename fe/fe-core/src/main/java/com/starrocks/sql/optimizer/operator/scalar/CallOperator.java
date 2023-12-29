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

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * Scalar operator support function call
 */
public class CallOperator extends ScalarOperator {
    private String fnName;
    /**
     * TODO:
     * We need a FunctionHandle to store the required information
     * to determine a unique function signature
     */
    //private final FunctionSignature signature;

    protected List<ScalarOperator> arguments;

    private Function fn;
    // The flag for distinct function
    private boolean isDistinct;

    // Ignore nulls.
    private boolean ignoreNulls = false;

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

    public void setFunction(Function fn) {
        this.fn = fn;
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

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet used = new ColumnRefSet();
        for (ScalarOperator child : arguments) {
            used.union(child.getUsedColumns());
        }
        return used;
    }

    @Override
    public boolean isConstant() {
        if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
            return false;
        }
        for (ScalarOperator child : getChildren()) {
            if (!child.isConstant()) {
                return false;
            }
        }
        return true;
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
                Objects.equals(arguments, other.arguments) &&
                Objects.equals(fn, other.fn);
    }


    // Only used for meaning equivalence comparison in iceberg table scan predicate
    @Override
    public boolean equivalent(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CallOperator other = (CallOperator) obj;
        if (this.arguments.size() != other.arguments.size()) {
            return false;
        }

        return isDistinct == other.isDistinct &&
                Objects.equals(fnName, other.fnName) &&
                Objects.equals(type, other.type) &&
                Objects.equals(fn, other.fn) &&
                IntStream.range(0, this.arguments.size())
                        .allMatch(i -> this.arguments.get(i).equivalent(other.arguments.get(i)));

    }

    @Override
    public ScalarOperator clone() {
        CallOperator operator = (CallOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        // copy fn
        if (this.fn != null) {
            operator.fn = this.fn.copy();
        }
        operator.fnName = this.fnName;
        operator.isDistinct = this.isDistinct;
        operator.ignoreNulls = this.ignoreNulls;
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCall(this, context);
    }
}
