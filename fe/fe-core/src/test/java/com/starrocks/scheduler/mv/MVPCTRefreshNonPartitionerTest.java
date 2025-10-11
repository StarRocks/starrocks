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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
<<<<<<< HEAD
=======
import com.starrocks.sql.common.PCellSortedSet;
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MVPCTRefreshNonPartitionerTest {

    @Test
    public void testGetAdaptivePartitionRefreshNumber() throws MVAdaptiveRefreshException {
        MvTaskRunContext mvTaskRunContext = Mockito.mock(MvTaskRunContext.class);
        TaskRunContext taskRunContext = Mockito.mock(TaskRunContext.class);
        Database database = Mockito.mock(Database.class);
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        MVPCTRefreshNonPartitioner job = new MVPCTRefreshNonPartitioner(mvTaskRunContext, taskRunContext,
<<<<<<< HEAD
                database, mv);
        Iterator<String> dummyIter = Collections.emptyIterator();

        int result = job.getAdaptivePartitionRefreshNumber(dummyIter);
=======
                database, mv, mvRefreshParams);
        int result = job.getAdaptivePartitionRefreshNumber(PCellSortedSet.of());
>>>>>>> ee66eb3b3f ([Enhancement] Change default_mv_partition_refresh_strategy to adaptive by default (#63594))
        Assertions.assertEquals(0, result);
    }
}
