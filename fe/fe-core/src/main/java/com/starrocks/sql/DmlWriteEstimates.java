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

package com.starrocks.sql;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;

final class DmlWriteEstimates {
    private DmlWriteEstimates() {
    }

    // Size only the final output: UPDATE writes its projected columns (including keys and generated
    // columns), while DELETE writes keys, any generated partition columns, and the operation column.
    // Scan bytes can include untouched columns and rows removed by filters or joins.
    static long fromExecPlan(ExecPlan execPlan) {
        OptExpression root = execPlan.getPhysicalPlan();
        if (root == null || root.getStatistics() == null || execPlan.getOutputColumns().isEmpty()) {
            return -1;
        }
        Statistics statistics = root.getStatistics();
        double rows = statistics.getOutputRowCount();
        // The optimizer floors even an unreported table row count at one. That is not evidence of a
        // small write. Unreliable row counts retain the sink's existing unknown-size fallback.
        if (statistics.isTableRowCountMayInaccurate() || !Double.isFinite(rows) || rows <= 0) {
            return -1;
        }
        double rowBytes = 0;
        for (ColumnRefOperator column : execPlan.getOutputColumns()) {
            ColumnStatistic columnStatistic = statistics.getColumnStatistics().get(column);
            // Match Statistics.getAvgRowSize(): tablet row counts can be reliable before column
            // statistics are collected, so use the type width for an unknown output column.
            double width = columnStatistic == null || columnStatistic.isUnknown()
                    ? column.getType().getTypeSize() : columnStatistic.getAverageRowSize();
            if (!Double.isFinite(width) || width < 0) {
                return -1;
            }
            rowBytes += width;
        }
        if (!Double.isFinite(rowBytes) || rowBytes <= 0) {
            return -1;
        }
        // A double-to-long conversion saturates at Long.MAX_VALUE instead of overflowing. The
        // estimate only changes writer width; it never changes the rows or columns being written.
        return (long) Math.ceil(rows * rowBytes);
    }
}
