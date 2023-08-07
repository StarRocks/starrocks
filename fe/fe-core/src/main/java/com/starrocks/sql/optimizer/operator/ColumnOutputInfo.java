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

/**
 * ColumnOutputInfo is used to describe an output column info in the output row of an operator.
 * It records the columnRef -> ScalarOperator pair.
 * It's a wrapper of Map.Entry<ColumnRefOperator, ScalarOperator> and provide some handy methods.
 */
public class ColumnOutputInfo {

    private final ColumnRefOperator columnRefOperator;

    private final ScalarOperator scalarOperator;

    private final int colId;

    public ColumnOutputInfo(ColumnRefOperator columnRefOperator, ScalarOperator scalarOperator) {
        this.columnRefOperator = columnRefOperator;
        this.colId = columnRefOperator.getId();
        this.scalarOperator = scalarOperator;
    }

    public ColumnOutputInfo(Map.Entry<ColumnRefOperator, ScalarOperator> entry) {
        this.columnRefOperator = entry.getKey();
        this.colId = entry.getKey().getId();
        this.scalarOperator = entry.getValue();
    }

    public ColumnRefOperator getColumnRef() {
        return columnRefOperator;
    }

    public int getColId() {
        return colId;
    }

    public ScalarOperator getScalarOp() {
        return scalarOperator;
    }

    public ColumnRefSet getUsedColumns() {
        return scalarOperator.getUsedColumns();
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
        if (!(obj instanceof ColumnOutputInfo)) {
            return false;
        }

        ColumnOutputInfo that = (ColumnOutputInfo) obj;

        return colId == that.colId;
    }

    @Override
    public String toString() {
        return columnRefOperator + " <- " + scalarOperator;
    }
}
