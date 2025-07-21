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


package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.InsertStmt;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class InsertOverwriteJobTest {
    @Test
    public void testBasic(@Injectable InsertStmt insertStmt) {
        new Expectations() {
            {
                insertStmt.getTargetPartitionIds();
                result = Lists.newArrayList(10L, 20L, 30L);
            }
        };
        InsertOverwriteJob insertOverwriteJob1 = new InsertOverwriteJob(100L, insertStmt, 110L, 120L,
                WarehouseManager.DEFAULT_WAREHOUSE_ID, false);
        Assertions.assertEquals(100L, insertOverwriteJob1.getJobId());
        Assertions.assertEquals(110L, insertOverwriteJob1.getTargetDbId());
        Assertions.assertEquals(120L, insertOverwriteJob1.getTargetTableId());
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, insertOverwriteJob1.getJobState());
        Assertions.assertEquals(Lists.newArrayList(10L, 20L, 30L), insertOverwriteJob1.getSourcePartitionIds());
        Assertions.assertFalse(insertOverwriteJob1.isFinished());
        insertOverwriteJob1.setJobState(InsertOverwriteJobState.OVERWRITE_SUCCESS);
        Assertions.assertTrue(insertOverwriteJob1.isFinished());

        List<Long> targetPartitionIds = Lists.newArrayList(10L, 20L, 30L);
        InsertOverwriteJob insertOverwriteJob2 = new InsertOverwriteJob(100L, 110L, 120L, targetPartitionIds, false);
        Assertions.assertEquals(100L, insertOverwriteJob2.getJobId());
        Assertions.assertEquals(110L, insertOverwriteJob2.getTargetDbId());
        Assertions.assertEquals(120L, insertOverwriteJob2.getTargetTableId());
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, insertOverwriteJob2.getJobState());
        Assertions.assertEquals(Lists.newArrayList(10L, 20L, 30L), insertOverwriteJob2.getSourcePartitionIds());
        Assertions.assertFalse(insertOverwriteJob2.isFinished());
    }
}
