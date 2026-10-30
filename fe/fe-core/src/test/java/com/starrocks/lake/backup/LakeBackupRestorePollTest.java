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

package com.starrocks.lake.backup;

import com.google.common.collect.Lists;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.proto.LockTabletMetadataResponse;
import com.starrocks.proto.RestoreSnapshotsResponse;
import com.starrocks.proto.UploadSnapshotsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Each run of a lake backup or restore job polls the responses of the RPCs it has sent. A response
 * that has not arrived yet must be polled again on the next run; dropping it leaves its task
 * unfinished forever and the job waits until it times out.
 */
public class LakeBackupRestorePollTest {

    @Test
    public void testBackupKeepsPendingLockResponses() throws Exception {
        LakeBackupJob job = new LakeBackupJob("label", 1L, "db", Lists.newArrayList(), 3600_000L, null, 1L);
        SnapshotInfo done = new SnapshotInfo(1L, 2L, 3L, 4L, 100L, 5L, 0, "", Lists.newArrayList());
        SnapshotInfo pending = new SnapshotInfo(1L, 2L, 3L, 4L, 101L, 5L, 0, "", Lists.newArrayList());

        Map<Long, Long> unfinished = field(BackupJob.class, job, "unfinishedTaskIds");
        unfinished.put(100L, 1L);
        unfinished.put(101L, 1L);
        Map<SnapshotInfo, Future<LockTabletMetadataResponse>> responses = field(LakeBackupJob.class, job,
                "lockResponses");
        responses.put(done, CompletableFuture.completedFuture(new LockTabletMetadataResponse()));
        CompletableFuture<LockTabletMetadataResponse> slow = new CompletableFuture<>();
        responses.put(pending, slow);

        job.waitingAllSnapshotsFinished();

        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertFalse(unfinished.containsKey(100L));
        Map<Long, SnapshotInfo> snapshotInfos = field(BackupJob.class, job, "snapshotInfos");
        Assertions.assertSame(done, snapshotInfos.get(100L));
        Assertions.assertTrue(unfinished.containsKey(101L));
        Assertions.assertSame(slow, responses.get(pending));
        Assertions.assertEquals(1, responses.size());
    }

    @Test
    public void testBackupKeepsPendingUploadResponses() throws Exception {
        LakeBackupJob job = new LakeBackupJob("label", 1L, "db", Lists.newArrayList(), 3600_000L, null, 1L);

        Map<Long, Long> unfinished = field(BackupJob.class, job, "unfinishedTaskIds");
        unfinished.put(10L, 1L);
        unfinished.put(11L, 1L);
        Map<Long, Future<UploadSnapshotsResponse>> responses = field(LakeBackupJob.class, job, "uploadResponses");
        responses.put(10L, CompletableFuture.completedFuture(new UploadSnapshotsResponse()));
        CompletableFuture<UploadSnapshotsResponse> slow = new CompletableFuture<>();
        responses.put(11L, slow);

        job.waitingAllUploadingFinished();

        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertFalse(unfinished.containsKey(10L));
        Assertions.assertTrue(unfinished.containsKey(11L));
        Assertions.assertSame(slow, responses.get(11L));
        Assertions.assertEquals(1, responses.size());
    }

    @Test
    public void testBackupFailsOnFailedResponse() throws Exception {
        LakeBackupJob job = new LakeBackupJob("label", 1L, "db", Lists.newArrayList(), 3600_000L, null, 1L);

        Map<Long, Long> unfinished = field(BackupJob.class, job, "unfinishedTaskIds");
        unfinished.put(10L, 1L);
        Map<Long, Future<UploadSnapshotsResponse>> responses = field(LakeBackupJob.class, job, "uploadResponses");
        responses.put(10L, CompletableFuture.failedFuture(new RuntimeException("upload failed")));

        job.waitingAllUploadingFinished();

        Assertions.assertFalse(job.getStatus().ok());
        Assertions.assertTrue(unfinished.containsKey(10L));
    }

    @Test
    public void testRestoreKeepsPendingResponses() throws Exception {
        LakeRestoreJob job = new LakeRestoreJob("label", "ts", 1L, "db", null, false, 1, 3600_000L, null, 1L,
                null, null);

        Map<Long, Long> unfinished = field(RestoreJob.class, job, "unfinishedSignatureToId");
        unfinished.put(20L, 1L);
        unfinished.put(21L, 1L);
        Map<Long, Future<RestoreSnapshotsResponse>> responses = field(LakeRestoreJob.class, job, "responses");
        responses.put(20L, CompletableFuture.completedFuture(new RestoreSnapshotsResponse()));
        CompletableFuture<RestoreSnapshotsResponse> slow = new CompletableFuture<>();
        responses.put(21L, slow);

        job.waitingAllDownloadFinished();

        Assertions.assertTrue(job.getStatus().ok(), job.getStatus().toString());
        Assertions.assertFalse(unfinished.containsKey(20L));
        Assertions.assertTrue(unfinished.containsKey(21L));
        Assertions.assertSame(slow, responses.get(21L));
        Assertions.assertEquals(1, responses.size());
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Class<?> owner, Object target, String name) throws Exception {
        Field f = owner.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(target);
    }
}
