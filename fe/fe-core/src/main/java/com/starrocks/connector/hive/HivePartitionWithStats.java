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

public class HivePartitionWithStats {
    String partitionName;
    HivePartition hivePartition;
    HivePartitionStats hivePartitionStats;

    public HivePartitionWithStats(String partitionName, HivePartition hivePartition, HivePartitionStats hivePartitionStats) {
        this.partitionName = partitionName;
        this.hivePartition = hivePartition;
        this.hivePartitionStats = hivePartitionStats;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public HivePartition getHivePartition() {
        return hivePartition;
    }

    public HivePartitionStats getHivePartitionStats() {
        return hivePartitionStats;
    }
}
