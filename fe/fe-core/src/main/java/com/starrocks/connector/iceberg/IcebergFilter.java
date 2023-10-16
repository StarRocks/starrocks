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

package com.starrocks.connector.iceberg;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class IcebergFilter {
    private final String databaseName;
    private final String tableName;
    private final long snapshotId;
    private final ScalarOperator predicate;

    public static IcebergFilter of(String databaseName, String tableName, long snapshotId, ScalarOperator predicate) {
        return new IcebergFilter(databaseName, tableName, snapshotId, predicate == null ? ConstantOperator.TRUE : predicate);
    }

    public IcebergFilter(String databaseName, String tableName, long snapshotId, ScalarOperator predicate) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.snapshotId = snapshotId;
        this.predicate = predicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergFilter that = (IcebergFilter) o;
        return snapshotId == that.snapshotId &&
                Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(tableName, that.tableName) &&
                predicate.equals(that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, snapshotId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IcebergFilter{");
        sb.append("databaseName='").append(databaseName).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append(", snapshotId=").append(snapshotId);
        sb.append(", predicate=").append(predicate);
        sb.append('}');
        return sb.toString();
    }
}
