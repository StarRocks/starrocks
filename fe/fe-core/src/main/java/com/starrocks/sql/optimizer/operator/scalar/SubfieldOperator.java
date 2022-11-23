// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SubfieldOperator extends ScalarOperator {

    // Only one child
    private final List<ScalarOperator> children = new ArrayList<>();
    private final String fieldName;

    // Build based on SubfieldExpr
    public static SubfieldOperator build(ScalarOperator child, SubfieldExpr expr) {
        return new SubfieldOperator(child, expr.getType(), expr.getFieldName());
    }

    // Build based on SlotRef which contains struct subfield access information
    public static SubfieldOperator build(ScalarOperator child, Type type, List<Integer> usedSubfieldPos) {
        Type tmpType = type;
        SubfieldOperator res = null;
        // Like SELECT a.b.c FROM tbl; Will be converted to:
        // Subfield(Subfield(ColumnRefOperator(a), "b"), "c")
        for (int pos : usedSubfieldPos) {
            StructType tmp = (StructType) tmpType;
            StructField field = tmp.getField(pos);
            tmpType = field.getType();
            if (res == null) {
                res = new SubfieldOperator(child, field.getType(), field.getName());
            } else {
                res = new SubfieldOperator(res, field.getType(), field.getName());
            }
        }

        Preconditions.checkArgument(res != null);
        return res;
    }

    private SubfieldOperator(ScalarOperator child, Type type, String fieldName) {
        super(OperatorType.SUBFIELD, type);
        this.children.add(child);
        this.fieldName = fieldName.toLowerCase();
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return children;
    }

    @Override
    public ScalarOperator getChild(int index) {
        Preconditions.checkArgument(index == 0);
        return children.get(0);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        Preconditions.checkArgument(index == 0);
        children.set(0, child);
    }

    @Override
    public String toString() {
        return "struct" + getChild(0).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChild(0), fieldName);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof SubfieldOperator)) {
            return false;
        }
        SubfieldOperator otherOp = (SubfieldOperator) other;
        return fieldName.equals(otherOp.fieldName) && getChild(0).equals(otherOp.getChild(0));
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitSubfield(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return getChild(0).getUsedColumns();
    }
}
