// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DictMappingOperator extends ScalarOperator {

    private final ColumnRefOperator dictColumn;
    private final ScalarOperator originScalaOperator;

    public DictMappingOperator(ColumnRefOperator dictColumn, ScalarOperator originScalaOperator, Type retType) {
        super(OperatorType.DICT_MAPPING, retType);
        this.dictColumn = dictColumn;
        this.originScalaOperator = originScalaOperator;
    }

    public ColumnRefOperator getDictColumn() {
        return dictColumn;
    }

    public ScalarOperator getOriginScalaOperator() {
        return originScalaOperator;
    }

    @Override
    public boolean isNullable() {
        return originScalaOperator.isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    @Override
    public String toString() {
        return "DictMapping(" + dictColumn + "{" + originScalaOperator + "}" + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(dictColumn, originScalaOperator);
    }

    @Override
    public boolean equals(Object other) {
        return false;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitDictMappingOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return dictColumn.getUsedColumns();
    }
}
