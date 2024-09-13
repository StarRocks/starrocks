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
package com.starrocks.meta;

public class TabletHierarchyId {
    public final long dbId;
    public final long tableId;
    public final long partitionId;
    public final long physicalPartitionId;
    public final long materializedIndexId;
    public final long tabletId;

    public TabletHierarchyId(long dbId, long tableId, long partitionId, long physicalPartitionId, long materializedIndexId,
                             long tabletId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.physicalPartitionId = physicalPartitionId;
        this.materializedIndexId = materializedIndexId;
        this.tabletId = tabletId;
    }
}
