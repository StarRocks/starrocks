// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public String getFnName() {
        return fnName;
    }

    public Function getFunction() {
        return fn;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountStar() {
        return fnName.equals("count") && arguments.isEmpty();
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
        if (fnName.equals("add")) {
            return getChild(0).debugString() + " + " + getChild(1).debugString();
        } else if (fnName.equals("subtract")) {
            return getChild(0).debugString() + " - " + getChild(1).debugString();
        } else if (fnName.equals("multiply")) {
            return getChild(0).debugString() + " * " + getChild(1).debugString();
        } else if (fnName.equals("divide")) {
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

    @Override
    public ScalarOperator clone() {
        CallOperator operator = (CallOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCall(this, context);
    }
}
