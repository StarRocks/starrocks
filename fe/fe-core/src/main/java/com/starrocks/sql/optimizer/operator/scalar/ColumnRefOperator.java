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

import com.starrocks.catalog.Type;
import com.starrocks.common.util.StringUtils;
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
    private String name;
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

    public void setName(String name) {
        this.name = name;
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
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
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

        ColumnRefOperator rightColumn = (ColumnRefOperator) obj;
        return StringUtils.areColumnNamesEqual(this.getName(), rightColumn.getName())
                && this.getType().equals(rightColumn.getType())
                && this.isNullable() == rightColumn.isNullable();
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
