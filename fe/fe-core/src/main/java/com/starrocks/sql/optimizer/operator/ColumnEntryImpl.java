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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class ColumnEntryImpl implements ColumnEntry {

    private final ColumnRefOperator columnRefOperator;

    private final ScalarOperator scalarOperator;

    private final int colId;

    public ColumnEntryImpl(ColumnRefOperator columnRefOperator, ScalarOperator scalarOperator) {
        this.columnRefOperator = columnRefOperator;
        this.colId = columnRefOperator.getId();
        this.scalarOperator = scalarOperator;
    }

    public ColumnEntryImpl(Map.Entry<ColumnRefOperator, ScalarOperator> entry) {
        this.columnRefOperator = entry.getKey();
        this.colId = entry.getKey().getId();
        this.scalarOperator = entry.getValue();
    }

    @Override
    public ColumnRefOperator getColumnRef() {
        return columnRefOperator;
    }

    @Override
    public int getColId() {
        return colId;
    }

    @Override
    public ScalarOperator getScalarOp() {
        return scalarOperator;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return scalarOperator.getUsedColumns();
    }

    @Override
    public ColumnRefOperator getKey() {
        return getColumnRef();
    }

    @Override
    public ScalarOperator getValue() {
        return getScalarOp();
    }

    @Override
    public ScalarOperator setValue(ScalarOperator value) {
        return null;
    }

    @Override
    public int hashCode() {
        return colId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ColumnEntryImpl)) {
            return false;
        }

        ColumnEntryImpl that = (ColumnEntryImpl) obj;

        return colId == that.colId;
    }

    @Override
    public String toString() {
        return columnRefOperator + " -> " + scalarOperator;
    }
}
