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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.proto.CompactStat;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class CompactionJobTest {
    @Test
    public void testBasic() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, 1, new MaterializedIndex());
        Quantiles scoreBefore = new Quantiles(5.0, 5.0, 8.0);
        ComputeResource computeResource = new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return 1L;
            }

            @Override
            public long getWorkerGroupId() {
                return 2L;
            }
        };
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, computeResource, "wh1", scoreBefore);

        Assertions.assertEquals(10010, job.getTxnId());
        Assertions.assertTrue(job.getAllowPartialSuccess());
        Assertions.assertEquals(scoreBefore, job.getScoreBefore());
        Assertions.assertNull(job.getScoreAfter());
        Assertions.assertFalse(job.isPartialSuccess());

        // before finish: debugString should contain scoreBefore, not profile
        String debugString = job.getDebugString();
        Assertions.assertTrue(debugString.contains("txnId=10010"));
        Assertions.assertTrue(debugString.contains("warehouse=wh1"));
        Assertions.assertTrue(debugString.contains("cnGroup=2"));
        Assertions.assertTrue(debugString.contains("scoreBefore="));
        Assertions.assertFalse(debugString.contains("profile="));

        job.setScoreAfter(new Quantiles(1.0, 1.0, 2.0));
        Assertions.assertNotNull(job.getScoreAfter());

        // after finish: debugString should contain profile, not scoreBefore
        job.finish();
        debugString = job.getDebugString();
        Assertions.assertTrue(debugString.contains("txnId=10010"));
        Assertions.assertTrue(debugString.contains("warehouse=wh1"));
        Assertions.assertTrue(debugString.contains("cnGroup=2"));
        Assertions.assertTrue(debugString.contains("profile="));
        Assertions.assertFalse(debugString.contains("scoreBefore="));
    }

    @Test
    public void testGetResult() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, 1, new MaterializedIndex());
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "", null);

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
        PhysicalPartition partition = new PhysicalPartition(0, 1, new MaterializedIndex());
        CompactionJob job = new CompactionJob(db, table, partition, 10010, false, null, "", null);
        assertDoesNotThrow(() -> {
            job.buildTabletCommitInfo();
        });
    }

    @Test
    public void testGetExecutionProfile() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, 1, new MaterializedIndex());
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "", null);

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

        // call again to verify cached profile is returned
        String s2 = job.getExecutionProfile();
        Assertions.assertEquals(s, s2);
    }

    @Test
    public void testSuccessCompactInputFIleSize() {
        Database db = new Database();
        Table table = new Table(Table.TableType.CLOUD_NATIVE);
        PhysicalPartition partition = new PhysicalPartition(0, 1, new MaterializedIndex());
        CompactionJob job = new CompactionJob(db, table, partition, 10010, true, null, "", null);

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
