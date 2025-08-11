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
import com.starrocks.catalog.Table;
import com.starrocks.proto.CompactStat;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CompactionJobTest {
    @Test
    public void testGetResult() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, "", 1, null);
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "");

        Assertions.assertTrue(job.getAllowPartialSuccess());
        List<CompactionTask> list = new ArrayList<>();
        list.add(new CompactionTask(100));
        list.add(new CompactionTask(101));
        job.setTasks(list);
        new MockUp<CompactionTask>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NOT_FINISHED;
            }
        };
        Assertions.assertEquals(CompactionTask.TaskResult.NOT_FINISHED, job.getResult());

        new MockUp<CompactionTask>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NONE_SUCCESS;
            }
        };
        Assertions.assertEquals(CompactionTask.TaskResult.NONE_SUCCESS, job.getResult());

        new MockUp<CompactionTask>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.ALL_SUCCESS;
            }
        };
        Assertions.assertEquals(CompactionTask.TaskResult.ALL_SUCCESS, job.getResult());

        new MockUp<CompactionTask>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.PARTIAL_SUCCESS;
            }
        };
        Assertions.assertEquals(CompactionTask.TaskResult.PARTIAL_SUCCESS, job.getResult());
    }

    @Test
    public void testBuildTabletCommitInfo() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, "", 1, null);
        CompactionJob job = new CompactionJob(db, table, partition, 10010, false, null, "");
        assertDoesNotThrow(() -> {
            job.buildTabletCommitInfo();
        });
    }

    @Test
    public void testGetExecutionProfile() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, "", 1, null);
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "");

        Assertions.assertTrue(job.getExecutionProfile().isEmpty());
        job.setAggregateTask(new CompactionTask(100));
        job.finish();
        new MockUp<CompactionTask>() {
            @Mock
            public List<CompactStat> getCompactStats() {
                List<CompactStat> list = new ArrayList<>();
                CompactStat stat = new CompactStat();
                stat.subTaskCount = 1;
                stat.readTimeRemote = 2L;
                stat.readBytesRemote = 3L;
                stat.readTimeLocal = 4L;
                stat.readBytesLocal = 5L;
                stat.readSegmentCount = 6L;
                stat.writeSegmentCount = 7L;
                stat.writeSegmentBytes = 8L;
                stat.writeTimeRemote = 9L;
                stat.inQueueTimeSec = 10;
                list.add(stat);
                return list;
            }
        };

        String s = job.getExecutionProfile();
        Assertions.assertFalse(s.isEmpty());
    }

    @Test
    public void testSuccessCompactInputFIleSize() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, "", 1, null);
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "");

        Assertions.assertTrue(job.getAllowPartialSuccess());
        List<CompactionTask> list = new ArrayList<>();
        list.add(new CompactionTask(100));
        list.add(new CompactionTask(101));
        job.setTasks(list);
        new MockUp<CompactionTask>() {
            @Mock
            public CompactionTask.TaskResult getResult() {
                return CompactionTask.TaskResult.NOT_FINISHED;
            }
        };
        Assertions.assertEquals(job.getSuccessCompactInputFileSize(), 0);
    }
}
