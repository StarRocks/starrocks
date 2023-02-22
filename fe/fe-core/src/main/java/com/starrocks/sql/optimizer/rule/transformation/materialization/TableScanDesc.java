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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;

import java.util.Objects;

public class TableScanDesc {
    private final Table table;
    // there may have multi same tables in the query. so assign it an index to distinguish them
    private final int index;
    private final LogicalScanOperator scanOperator;
    // join type of LogicalJoinOperator above scan operator
    private final JoinOperator parentJoinType;

    public TableScanDesc(Table table, int index, LogicalScanOperator scanOperator, JoinOperator parentJoinType) {
        this.table = table;
        this.index = index;
        this.scanOperator = scanOperator;
        this.parentJoinType = parentJoinType;
    }

    public Table getTable() {
        return table;
    }

    public int getIndex() {
        return index;
    }

    public JoinOperator getParentJoinType() {
        return parentJoinType;
    }

    public String getName() {
        return table.getName();
    }

    public LogicalScanOperator getScanOperator() {
        return scanOperator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableScanDesc that = (TableScanDesc) o;
        return Objects.equals(table, that.table) && index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, index);
    }
}
