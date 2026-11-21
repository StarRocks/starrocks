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

package com.starrocks.persist.image;

import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupHandler;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.Status;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.proto.AbstractJobPB;
import com.starrocks.persist.proto.BackupMgrPB;
import com.starrocks.persist.proto.RepositoryMgrPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackupHandlerImageV3Test {
    @TempDir
    Path tempDir;

    private Path save(BackupHandler handler) throws Exception {
        Path file = tempDir.resolve("backup-" + System.nanoTime());
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 1)) {
            handler.save(writer);
            writer.finish();
        }
        return file;
    }

    private Path entries(List<AbstractJobPB> entries) throws Exception {
        Path file = tempDir.resolve("entries-" + System.nanoTime());
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 1)) {
            try (SectionWriter section = writer.beginSection(SectionId.BACKUP_MGR)) {
                section.writeManager(BackupMgrPB.newBuilder().setRepoMgr(RepositoryMgrPB.getDefaultInstance()).build());
                for (AbstractJobPB entry : entries) {
                    section.writeEntry(entry);
                }
            }
            writer.finish();
        }
        return file;
    }

    private BackupHandler load(Path path) throws Exception {
        BackupHandler handler = new BackupHandler();
        try (ImageReaderV3 reader = new ImageReaderV3(path)) {
            reader.load(handler.getImageV3Loaders());
        }
        return handler;
    }

    @Test
    public void testJobsExpiryAndEmptyManager() throws Exception {
        int oldSeconds = Config.history_job_keep_max_second;
        Config.history_job_keep_max_second = 3600;
        try {
            long now = System.currentTimeMillis();
            GlobalStateMgr global = Mockito.mock(GlobalStateMgr.class);
            BackupHandler live = new BackupHandler(global);
            Map<Long, AbstractJob> map = getField(live, "dbIdToBackupOrRestoreJob");
            List<AbstractJob> jobs = BackupJobProtoTest.jobs(now);
            for (AbstractJob job : jobs) {
                map.put(job.getDbId(), job);
            }
            BackupJob external = new BackupJob("catalog", -1, null, List.of(), 1000, null, 77);
            map.put(-1L, external);
            BackupJob expired = new BackupJob("expired", 9, "db9", List.of(), 1000, null, 77);
            setField(expired, "state", BackupJob.BackupJobState.FINISHED);
            setField(expired, "finishedTime", now - 7_200_000);
            map.put(9L, expired);
            UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
            live.saveBackupHandlerV2(v2Image.getImageWriter());
            BackupHandler v2 = new BackupHandler();
            v2.loadBackupHandlerV2(v2Image.getMetaBlockReader());
            assertNull(v2.getJob(9));
            Path file = save(live);
            BackupHandler loaded = load(file);
            assertNull(loaded.getJob(9));
            assertNotNull(loaded.getJob(-1));
            for (AbstractJob job : jobs) {
                BackupJobProtoTest.assertJobEquivalent(job, loaded.getJob(job.getDbId()));
                BackupJobProtoTest.assertJobEquivalent(job, v2.getJob(job.getDbId()));
                assertEquals(job.getClass(), loaded.getJob(job.getDbId()).getClass());
            }
            assertFalse(loaded.isRunning());
            assertFalse(loaded.getRepoMgr().isRunning());

            Mockito.verifyNoInteractions(global);
            BackupHandler empty = load(save(new BackupHandler()));
            assertTrue(((Map<?, ?>) getField(empty, "dbIdToBackupOrRestoreJob")).isEmpty());
        } finally {
            Config.history_job_keep_max_second = oldSeconds;
        }
    }

    @Test
    public void testRepositoryIndexesAndPathProbe() throws Exception {
        List<String> paths = new ArrayList<>();
        List<Long> threads = new ArrayList<>();
        new MockUp<BlobStorage>() {
            @Mock
            public Status checkPathExist(String path) {
                paths.add(path);
                threads.add(Thread.currentThread().getId());
                if (path.endsWith("__palo_repository_failure")) {
                    throw new IllegalStateException("probe failed");
                }
                return path.endsWith("__palo_repository_legacy") ? Status.OK : new Status(Status.ErrCode.NOT_FOUND, "missing");
            }
        };
        BackupHandler source = new BackupHandler();
        long id = 1;
        for (String name : List.of("legacy", "modern", "failure")) {
            BlobStorage storage = new BlobStorage("", Map.of("fs.s3a.endpoint", "endpoint"), false);
            source.getRepoMgr().replayAddRepo(new Repository(id++, name, true, "s3://bucket/root", storage));
        }
        BackupHandler loaded = load(save(source));
        assertEquals(3, paths.size());
        assertTrue(threads.stream().allMatch(thread -> thread == Thread.currentThread().getId()));
        id = 1;
        for (String name : List.of("legacy", "modern", "failure")) {
            Repository repo = loaded.getRepoMgr().getRepo(name);
            assertSame(repo, loaded.getRepoMgr().getRepo(id++));
            assertEquals(name.equals("legacy") ? "__palo_repository_" : "__starrocks_repository_",
                    getField(repo, "prefixRepo"));
            GsonEquivalence.assertEquivalent(source.getRepoMgr().getRepo(name), repo);
        }
        assertFalse(loaded.getRepoMgr().isRunning());
    }

    @Test
    public void testCheckpointSkipsRepositoryProbe() {
        AtomicInteger probes = new AtomicInteger();
        new MockUp<BlobStorage>() {
            @Mock
            public Status checkPathExist(String path) {
                probes.incrementAndGet();
                return Status.OK;
            }
        };
        Repository source = new Repository(1, "repo", false, "s3://bucket", new BlobStorage("broker", Map.of()));
        AtomicBoolean checkpoint = new AtomicBoolean(true);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isCheckpointThread() {
                return checkpoint.get();
            }
        };
        try {
            Repository restored = Repository.fromProto(source.toProto());
            assertEquals(0, probes.get());
            assertEquals("__starrocks_repository_", getField(restored, "prefixRepo"));
            GsonEquivalence.assertEquivalent(source, restored);
        } finally {
            checkpoint.set(false);
        }
        Repository.fromProto(source.toProto());
        assertEquals(1, probes.get());
    }

    @Test
    public void testFileOrderControlsSharedMvRestoreContext() throws Exception {
        LakeTable table = BackupMetadataProtoTest.lakeTable();
        table.getRelatedMaterializedViews().add(new MvId(80, 81));
        BackupJobInfo firstInfo = new BackupJobInfo();
        firstInfo.dbId = 90;
        firstInfo.dbName = "remote";
        firstInfo.backupTime = 100;
        BackupJobInfo.BackupTableInfo tableInfo = new BackupJobInfo.BackupTableInfo();
        tableInfo.id = table.getId();
        tableInfo.name = table.getName();
        firstInfo.tables.put(tableInfo.name, tableInfo);
        BackupJobInfo secondInfo = new BackupJobInfo();
        secondInfo.dbId = firstInfo.dbId;
        secondInfo.dbName = firstInfo.dbName;
        secondInfo.backupTime = 200;
        secondInfo.tables.put(tableInfo.name, tableInfo);
        RestoreJob first = new RestoreJob("first", "ts", 11, "local_first", firstInfo, false, 1,
                1000, null, 1, new BackupMeta(List.of(table)), null);
        RestoreJob second = new RestoreJob("second", "ts", 12, "local_second", secondInfo, false, 1,
                1000, null, 1, new BackupMeta(List.of(table)), null);
        Path file = entries(List.of(first.toProto(), second.toProto()));
        BackupHandler loaded = load(file);
        MvRestoreContext context = getField(loaded, "mvRestoreContext");
        var info = context.getMvBaseTableToBackupTableInfo().get(new TableName("remote", table.getName()));
        assertNotNull(info);
        assertEquals("local_second", info.getLocalDbName());
        assertEquals(200, info.getBackupTime());
        assertEquals(table.getId(), info.getRemoteTableId());
    }

    @Test
    public void testBackupTableWriteFailureKeepsCheckedIOException() throws Exception {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getName()).thenReturn("broken_table");
        IOException cause = new IOException("cannot encode table metadata");
        Mockito.when(table.toProto()).thenThrow(cause);
        BackupJob job = new BackupJob("backup", 1, "db", List.of(), 1000, null, 7);
        setField(job, "backupMeta", new BackupMeta(List.of(table)));
        BackupHandler handler = new BackupHandler();
        Map<Long, AbstractJob> jobs = getField(handler, "dbIdToBackupOrRestoreJob");
        jobs.put(1L, job);
        assertSame(cause, assertThrows(IOException.class, () -> save(handler)));
    }
}
