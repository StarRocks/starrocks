// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Scalar operator support variable, represent reference of column variable
 */
public final class ColumnRefOperator extends ScalarOperator {
    private final int id;
    private final String name;
    private boolean nullable;

    private boolean isLambdaArgument;

    public ColumnRefOperator(int id, Type type, String name, boolean nullable) {
        super(OperatorType.VARIABLE, type);
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.nullable = nullable;
        this.isLambdaArgument = false;
    }

    public ColumnRefOperator(int id, Type type, String name, boolean nullable, boolean isLambdaArgument) {
        super(OperatorType.VARIABLE, type);
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.nullable = nullable;
        this.isLambdaArgument = isLambdaArgument;
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
        if (isLambdaArgument) {
            return new ColumnRefSet();
        }
        return new ColumnRefSet(id);
    }

    @Override
    public String toString() {
        return id + ": " + name;
    }

    public static String toString(List<ColumnRefOperator> columns) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (ColumnRefOperator column : columns) {
            if (i++ != 0) {
                sb.append(",");
            }
            sb.append(column);
        }
        return sb.toString();
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

    /**
     * return default value "col" to eliminate the influence of
     * column ref id on UT, make ut code can be fuzzy matching
     */
    @Override
    public String debugString() {
        return "col";
    }
}
