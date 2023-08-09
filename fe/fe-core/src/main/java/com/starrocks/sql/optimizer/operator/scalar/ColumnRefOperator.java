// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Scalar operator support variable, represent reference of column variable
 */
public final class ColumnRefOperator extends ScalarOperator {
    private final int id;
    private final String name;
    private boolean nullable;

    public ColumnRefOperator(int id, Type type, String name, boolean nullable) {
        super(OperatorType.VARIABLE, type);
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.nullable = nullable;
    }

    public ColumnRefOperator(int id, Type type, String name, boolean nullable, boolean isLambdaArgument) {
        // lambda arguments cannot be seen by outer scopes, so set it a different operator type.
        super(isLambdaArgument ? OperatorType.LAMBDA_ARGUMENT : OperatorType.VARIABLE, type);
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.nullable = nullable;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isVariable() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        // variable scalar operator should be the leaf node
        return emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    public ColumnRefSet getUsedColumns() {
        if (getOpType().equals(OperatorType.LAMBDA_ARGUMENT)) {
            return new ColumnRefSet();
        }
        return new ColumnRefSet(id);
    }

    @Override
    public void getColumnRefs(List<ColumnRefOperator> columns) {
        columns.add(this);
    }

    @Override
    public String toString() {
        return id + ": " + name;
    }

    public static String toString(Collection<ColumnRefOperator> columns) {
        StringJoiner joiner = new StringJoiner("{", ", ", "}");
        for (ColumnRefOperator column : columns) {
            joiner.add(column.toString());
        }
        return joiner.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitVariableReference(this, context);
    }

    public static boolean equals(List<ColumnRefOperator> lhs, List<ColumnRefOperator> rhs) {
        if (lhs == null || rhs == null) {
            return lhs == null && rhs == null;
        }
        if (lhs == rhs) {
            return true;
        }
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (int i = 0; i < lhs.size(); ++i) {
            if (!lhs.get(i).equals(rhs.get(i))) {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnRefOperator)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final ColumnRefOperator column = (ColumnRefOperator) obj;
        // The column id is unique
        return id == column.id;
    }

    @Override
    public boolean equivalent(Object obj) {
        if (!(obj instanceof ColumnRefOperator)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        ColumnRefOperator leftColumn = (ColumnRefOperator) this;
        ColumnRefOperator rightColumn = (ColumnRefOperator) obj;
        return leftColumn.getName().equals(rightColumn.getName())
                && leftColumn.getType().equals(rightColumn.getType())
                && leftColumn.isNullable() == rightColumn.isNullable();
    }

    /**
     * return default value "col" to eliminate the influence of
     * column ref id on UT, make ut code can be fuzzy matching
     */
    @Override
    public String debugString() {
        return "col";
    }
}
