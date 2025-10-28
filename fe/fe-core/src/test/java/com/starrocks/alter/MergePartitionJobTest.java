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

package com.starrocks.alter;

import com.google.common.collect.Multimap;
import com.starrocks.alter.AlterCancelException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.scheduler.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MergePartitionJobTest {

    @Test
    public void testTempPartitionNotVisibleRaisesFailure() throws Exception {
        String tempPartitionName = "tmp_p1";
        MergePartitionJob job = new MergePartitionJob(1L, 2L, 3L, "tbl", 1000L) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return partitionId == 100L;
            }
        };

        Field nameMappingField = MergePartitionJob.class.getDeclaredField("tempPartitionNameToSourcePartitionNames");
        nameMappingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Multimap<String, String> nameMapping = (Multimap<String, String>) nameMappingField.get(job);
        nameMapping.put(tempPartitionName, "p1");

        OptimizeTask task = new OptimizeTask("task-1");
        task.setTempPartitionName(tempPartitionName);
        task.setPartitionName("p1");
        task.setLastVersion(0L);
        task.setOptimizeTaskState(Constants.TaskRunState.SUCCESS);
        job.getOptimizeTasks().add(task);

        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(2L);

        OlapTable table = Mockito.mock(OlapTable.class);
        Partition tempPartition = Mockito.mock(Partition.class);
        Mockito.when(tempPartition.getId()).thenReturn(100L);
        Mockito.when(tempPartition.getName()).thenReturn(tempPartitionName);
        Mockito.when(table.getPartition(tempPartitionName, true)).thenReturn(tempPartition);
        Mockito.doNothing().when(table).dropTempPartition(tempPartitionName, true);
        Mockito.when(table.getPartition("p1")).thenReturn(null);

        Method onFinished = MergePartitionJob.class.getDeclaredMethod("onFinished", Database.class, OlapTable.class);
        onFinished.setAccessible(true);

        AlterCancelException ex = Assertions.assertThrows(AlterCancelException.class, () -> {
            try {
                onFinished.invoke(job, db, table);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof AlterCancelException) {
                    throw (AlterCancelException) cause;
                }
                throw new RuntimeException(cause);
            }
        });
        Assertions.assertTrue(ex.getMessage().contains("rewrite failed"));

        Mockito.verify(table).dropTempPartition(tempPartitionName, true);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, task.getOptimizeTaskState());
        Assertions.assertTrue(nameMapping.isEmpty());
    }
}
