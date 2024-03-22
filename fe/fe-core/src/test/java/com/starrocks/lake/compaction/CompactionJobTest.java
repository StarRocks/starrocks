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

package com.starrocks.lake.compaction;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.PhysicalPartitionImpl;
import com.starrocks.catalog.Table;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CompactionJobTest {
    @Test
    public void testBuildTabletCommitInfo() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartitionImpl(0, 1, 2, null);
        CompactionJob job = new CompactionJob(db, table, partition, 10010);
        assertDoesNotThrow(() -> {
            job.buildTabletCommitInfo();
        });
    }
}
