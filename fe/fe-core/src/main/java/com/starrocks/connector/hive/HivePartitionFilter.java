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

package com.starrocks.connector.hive;

import com.google.common.base.Objects;
import com.starrocks.connector.DatabaseTableName;

public class HivePartitionFilter {
    private final DatabaseTableName tableName;
    private final String filter;

    public HivePartitionFilter(DatabaseTableName tableName, String filter) {
        this.tableName = tableName;
        this.filter = filter;
    }

    public static HivePartitionFilter of(DatabaseTableName tableName, String filter) {
        return new HivePartitionFilter(tableName, filter);
    }

    public DatabaseTableName getHiveTableName() {
        return tableName;
    }

    public String getFilter() {
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HivePartitionFilter)) {
            return false;
        }
        HivePartitionFilter that = (HivePartitionFilter) o;
        return Objects.equal(tableName, that.tableName) && Objects.equal(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, filter);
    }
}
