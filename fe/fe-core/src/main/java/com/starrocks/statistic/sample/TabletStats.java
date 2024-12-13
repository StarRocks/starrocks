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

package com.starrocks.statistic.sample;

public class TabletStats {

    private final long tabletId;
<<<<<<< HEAD
    private final long partitionId;
=======
>>>>>>> 291562ac40 ([Enhancement] Optimize the Chunk destructor (#53898))
    private final long rowCount;

    public TabletStats(long tabletId, long partitionId, long rowCount) {
        this.tabletId = tabletId;
<<<<<<< HEAD
        this.partitionId = partitionId;
=======
>>>>>>> 291562ac40 ([Enhancement] Optimize the Chunk destructor (#53898))
        this.rowCount = rowCount;
    }

    public long getTabletId() {
        return tabletId;
    }

<<<<<<< HEAD
    public long getPartitionId() {
        return partitionId;
    }

=======
>>>>>>> 291562ac40 ([Enhancement] Optimize the Chunk destructor (#53898))
    public long getRowCount() {
        return rowCount;
    }
}
