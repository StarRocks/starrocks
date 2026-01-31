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

package com.starrocks.lake.snapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

public class ClusterSnapshotCopyForPersistTest {
    @Test
    public void testClusterSnapshotCopyForPersist() {
        ClusterSnapshot snapshot = new ClusterSnapshot(1L, "snap",
                ClusterSnapshot.ClusterSnapshotType.MANUAL, "sv", 10L, 20L, 30L, 40L);
        ClusterSnapshot copy = snapshot.copyForPersist();
        Assertions.assertEquals(snapshot.getId(), copy.getId());
        Assertions.assertEquals(snapshot.getSnapshotName(), copy.getSnapshotName());
        Assertions.assertEquals(snapshot.getStorageVolumeName(), copy.getStorageVolumeName());
        Assertions.assertEquals(snapshot.getCreatedTimeMs(), copy.getCreatedTimeMs());
        Assertions.assertEquals(snapshot.getFinishedTimeMs(), copy.getFinishedTimeMs());
        Assertions.assertEquals(snapshot.getFeJournalId(), copy.getFeJournalId());
        Assertions.assertEquals(snapshot.getStarMgrJournalId(), copy.getStarMgrJournalId());
        Assertions.assertNotSame(snapshot, copy);
    }

    @Test
    public void testClusterSnapshotJobCopyForPersist() {
        ClusterSnapshotJob job = new ClusterSnapshotJob(2L, "snap-job", "sv", 10L);
        job.setState(ClusterSnapshotJob.ClusterSnapshotJobState.UPLOADING);
        job.setErrMsg("err");
        job.setDetailInfo("detail");
        ClusterSnapshotJob copy = job.copyForPersist();
        Assertions.assertEquals(job.getState(), copy.getState());
        Assertions.assertEquals(getField(job, "errMsg"), getField(copy, "errMsg"));
        Assertions.assertEquals(getField(job, "detailInfo"), getField(copy, "detailInfo"));
        Assertions.assertEquals(job.getSnapshot().getId(), copy.getSnapshot().getId());
        Assertions.assertNotSame(job, copy);
        Assertions.assertNotSame(job.getSnapshot(), copy.getSnapshot());
    }

    private static Object getField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception e) {
            throw new AssertionError("Failed to access field: " + fieldName, e);
        }
    }
}
