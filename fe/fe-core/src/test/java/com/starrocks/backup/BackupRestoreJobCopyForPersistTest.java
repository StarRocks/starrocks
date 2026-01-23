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

package com.starrocks.backup;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.TableName;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.TableRefPersist;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class BackupRestoreJobCopyForPersistTest {
    @Test
    public void testBackupJobCopyForPersist() throws Exception {
        BackupJob job = new BackupJob();
        setField(job, "type", AbstractJob.JobType.BACKUP);
        setField(job, "repoId", 10L);
        setField(job, "jobId", 11L);
        setField(job, "label", "label");
        setField(job, "dbId", 12L);
        setField(job, "dbName", "db");
        setField(job, "status", new Status(Status.ErrCode.BAD_FILE, "bad"));
        setField(job, "createTime", 100L);
        setField(job, "finishedTime", 200L);
        setField(job, "timeoutMs", 300L);
        Map<Long, String> backupTaskErrMsg = Maps.newHashMap();
        backupTaskErrMsg.put(1L, "err");
        setField(job, "taskErrMsg", backupTaskErrMsg);

        List<TableRefPersist> tableRefs = Lists.newArrayList(new TableRefPersist(new TableName("db", "tbl"), "t"));
        setField(job, "tableRefs", tableRefs);
        setField(job, "state", BackupJob.BackupJobState.SNAPSHOTING);
        setField(job, "snapshotFinishedTime", 400L);
        setField(job, "snapshotUploadFinishedTime", 500L);
        Map<Long, SnapshotInfo> snapshotInfoMap = Maps.newConcurrentMap();
        snapshotInfoMap.put(1L,
                new SnapshotInfo(1L, 2L, 3L, 4L, 5L, 6L, 7, "/path", Lists.newArrayList("f1")));
        setField(job, "snapshotInfos", snapshotInfoMap);
        setField(job, "backupMeta", new BackupMeta(Lists.newArrayList()));
        setField(job, "localMetaInfoFilePath", "/tmp/meta");
        setField(job, "localJobInfoFilePath", "/tmp/job");
        setField(job, "backupFunctions", Lists.newArrayList());
        setField(job, "backupCatalogs", Lists.newArrayList());

        BackupJob copy = job.copyForPersist();
        assertFieldsEqual(job, copy,
                "type",
                "repoId",
                "jobId",
                "label",
                "dbId",
                "dbName",
                "status",
                "createTime",
                "finishedTime",
                "timeoutMs",
                "taskErrMsg",
                "tableRefs",
                "state",
                "snapshotFinishedTime",
                "snapshotUploadFinishedTime",
                "snapshotInfos",
                "backupMeta",
                "localMetaInfoFilePath",
                "localJobInfoFilePath",
                "backupFunctions",
                "backupCatalogs");
    }

    @Test
    public void testRestoreJobCopyForPersist() throws Exception {
        RestoreJob job = new RestoreJob();
        setField(job, "type", AbstractJob.JobType.RESTORE);
        setField(job, "repoId", 10L);
        setField(job, "jobId", 11L);
        setField(job, "label", "label");
        setField(job, "dbId", 12L);
        setField(job, "dbName", "db");
        setField(job, "status", new Status(Status.ErrCode.BAD_FILE, "bad"));
        setField(job, "createTime", 100L);
        setField(job, "finishedTime", 200L);
        setField(job, "timeoutMs", 300L);
        Map<Long, String> restoreTaskErrMsg = Maps.newHashMap();
        restoreTaskErrMsg.put(1L, "err");
        setField(job, "taskErrMsg", restoreTaskErrMsg);

        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.name = "job";
        jobInfo.dbName = "db";
        jobInfo.dbId = 12L;
        setField(job, "backupTimestamp", "ts");
        setField(job, "jobInfo", jobInfo);
        setField(job, "allowLoad", true);
        setField(job, "state", RestoreJob.RestoreJobState.DOWNLOADING);
        setField(job, "backupMeta", new BackupMeta(Lists.newArrayList()));

        RestoreFileMapping fileMapping = new RestoreFileMapping();
        fileMapping.putMapping(new RestoreFileMapping.IdChain(1L, 2L, 3L, 4L, 5L),
                new RestoreFileMapping.IdChain(6L, 7L, 8L, 9L, 10L), true);
        setField(job, "fileMapping", fileMapping);
        setField(job, "metaPreparedTime", 400L);
        setField(job, "snapshotFinishedTime", 500L);
        setField(job, "downloadFinishedTime", 600L);
        setField(job, "restoreReplicationNum", 3);
        setField(job, "restoredPartitions", Lists.newArrayList());
        setField(job, "restoredTbls", Lists.newArrayList());

        HashBasedTable<Long, Long, Long> restoredVersionInfo = HashBasedTable.create();
        restoredVersionInfo.put(1L, 2L, 3L);
        setField(job, "restoredVersionInfo", restoredVersionInfo);

        HashBasedTable<Long, Long, SnapshotInfo> snapshotInfos = HashBasedTable.create();
        snapshotInfos.put(1L, 2L, new SnapshotInfo(1L, 2L, 3L, 4L, 5L, 6L, 7, "/path", Lists.newArrayList("f1")));
        setField(job, "snapshotInfos", snapshotInfos);

        setField(job, "colocatePersistInfos",
                Lists.newArrayList(ColocatePersistInfo.createForRemoveTable(10L)));

        RestoreJob copy = job.copyForPersist();
        assertFieldsEqual(job, copy,
                "type",
                "repoId",
                "jobId",
                "label",
                "dbId",
                "dbName",
                "status",
                "createTime",
                "finishedTime",
                "timeoutMs",
                "taskErrMsg",
                "backupTimestamp",
                "jobInfo",
                "allowLoad",
                "state",
                "backupMeta",
                "fileMapping",
                "metaPreparedTime",
                "snapshotFinishedTime",
                "downloadFinishedTime",
                "restoreReplicationNum",
                "restoredPartitions",
                "restoredTbls",
                "restoredVersionInfo",
                "snapshotInfos",
                "colocatePersistInfos");
    }

    private static void assertFieldsEqual(Object original, Object copy, String... fieldNames) throws Exception {
        for (String fieldName : fieldNames) {
            Object originalValue = getField(original, fieldName);
            Object copyValue = getField(copy, fieldName);
            Assertions.assertEquals(originalValue, copyValue, fieldName);
        }
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ex) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
