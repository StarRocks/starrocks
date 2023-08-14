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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class HivePartitionValue {
    // use empty list to represent all partition values when get partition names from partitionKeysCache
    public static final List<Optional<String>> ALL_PARTITION_VALUES = ImmutableList.of();

    private final HiveTableName tableName;

    private final List<Optional<String>> partitionValues;

    public HivePartitionValue(HiveTableName tableName, List<Optional<String>> partitionValues) {
        this.tableName = tableName;
        this.partitionValues = partitionValues;
    }

    public static HivePartitionValue of(HiveTableName tableName, List<Optional<String>> partitionValues) {
        return new HivePartitionValue(tableName, partitionValues);
    }

    public HiveTableName getHiveTableName() {
        return tableName;
    }

    public List<Optional<String>> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HivePartitionValue)) {
            return false;
        }
        HivePartitionValue that = (HivePartitionValue) o;
        return Objects.equal(tableName, that.tableName) &&
                Objects.equal(partitionValues, that.partitionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, partitionValues);
    }
}
