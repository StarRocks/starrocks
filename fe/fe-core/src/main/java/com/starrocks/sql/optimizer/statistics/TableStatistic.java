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
package com.starrocks.sql.optimizer.statistics;

import java.util.Objects;

public class TableStatistic {
    private final Long tableId;
    private final Long partitionId;
    private final Long rowCount;
    private static final TableStatistic UNKNOWN = new TableStatistic(-1L, -1L, 0L);

    public TableStatistic(Long tableId, Long partitionId, Long rowCount) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.rowCount = rowCount;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public static TableStatistic unknown() {
        return UNKNOWN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStatistic that = (TableStatistic) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(rowCount, that.rowCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, rowCount);
    }
}
