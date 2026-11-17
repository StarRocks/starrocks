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

import com.starrocks.common.Pair;
import com.starrocks.load.pipe.FilePipeSource;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.load.pipe.PipeTaskDesc;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.FilePipeSourcePB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.PipePB;
import com.starrocks.persist.proto.StringMapPB;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PipeMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
    }

    static Pipe pipe(long id, Pipe.State state) {
        Pipe pipe = GsonUtils.GSON.fromJson("""
                {
                  "name": "pipe-%d",
                  "id": {"dbId": 8, "id": %d},
                  "type": "FILE",
                  "state": "RUNNING",
                  "originSql": "insert into db.target select * from files('path'='s3://bucket/file','format'='parquet')",
                  "filePipeSource": {
                    "pipe_id": {"dbId": 8, "id": %d},
                    "path": "s3://bucket/file",
                    "format": "parquet",
                    "table_properties": {"path": "s3://bucket/file", "format": "parquet", "aws.s3.region": "region"},
                    "auto_ingest": false,
                    "batch_size": 12345,
                    "batch_files": 12,
                    "eos": true
                  },
                  "targetTable": {"tbl": "target", "fullDb": "db"},
                  "properties": {"poll_interval": "120", "auto_ingest": "false", "batch_size": "1048576",
                                 "batch_files": "7", "task.query_timeout": "123", "warehouse": "default_warehouse"},
                  "createdTime": 123456789,
                  "load_status": {"loadedFiles": 37, "loadedBytes": 4294967296, "loadingFiles": 9,
                                  "lastLoadedTime": 1000000},
                  "task_execution_variables": {"query_timeout": "1", "exec_mem_limit": "4294967296"}
                }
                """.formatted(id, id, id), Pipe.class);
        pipe.setState(state);
        return pipe;
    }

    private PipeManager restore(PipeManager source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source.getRepo()::save);
        PipeManager restored = new PipeManager();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getRepo().getImageV3Loaders());
        assertEquals(310, sections.get(0).getType());
        assertEquals(source.getAllPipes().size(), sections.get(0).getNumEntries());
        assertEquals(0, sections.get(0).getManagerLength());
        return restored;
    }

    @Test
    public void testAllFieldsAndStatesMatchV2() throws Exception {
        PipeManager source = new PipeManager();
        for (Pipe.State state : Pipe.State.values()) {
            Pipe pipe = pipe(state.ordinal() + 1, state);
            FinalManagersImageTestSupport.assertAllFieldsPresent(pipe);
            FinalManagersImageTestSupport.assertAllFieldsPresent(pipe.getPipeSource());
            source.putPipe(pipe);
        }
        PipeManager expected = new PipeManager();
        FinalManagersImageTestSupport.v2(source.getRepo()::save, expected.getRepo()::load);
        PipeManager restored = restore(source);
        for (Pipe value : expected.getAllPipes()) {
            Pipe actual = restored.getPipeById(value.getPipeId());
            GsonEquivalence.assertEquivalent(value, actual);
            assertEquals(123456789L, actual.getCreatedTime());
            assertEquals(120, actual.getPollIntervalSecond());
            assertEquals("123", actual.getTaskProperties().get("query_timeout"));
            assertEquals("default_warehouse", actual.getTaskProperties().get("warehouse"));
            assertFalse(actual.isRecovered());
            assertFalse(actual.isRunnable());
            assertTrue(actual.getRunningTasks().isEmpty());
            assertEquals(value.getState(), actual.getState());
        }
        Map<Pair<Long, String>, PipeId> names = getField(restored, "nameToId");
        assertEquals(4, names.size());
        assertEquals(new PipeId(8, 1), names.get(Pair.create(8L, "pipe-1")));

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testLoadStatusClearsOnlyRuntimeCountersLikeV2() throws Exception {
        Pipe source = pipe(1, Pipe.State.RUNNING);
        source.getLoadStatus().loadingFiles = 11;
        source.getLoadStatus().lastLoadedTime = LocalDateTime.of(2020, 3, 4, 5, 6, 7);
        source.getLoadStatus().loadRows = 200;
        PipePB pb = PipePB.parseFrom(source.toProto().toByteArray());
        assertEquals(11, pb.getLoadStatus().getLoadingFiles());
        assertTrue(pb.getLoadStatus().hasLastLoadedTime());
        Pipe restored = Pipe.fromProto(pb);
        Pipe expected = FinalManagersImageTestSupport.gson(source, Pipe.class);
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(37, restored.getLoadStatus().loadedFiles);
        assertEquals(4294967296L, restored.getLoadStatus().loadedBytes);
        assertEquals(0, restored.getLoadStatus().loadingFiles);
        assertEquals(0, restored.getLoadStatus().loadRows);
        assertNull(restored.getLoadStatus().lastLoadedTime);
        assertEquals(11, source.getLoadStatus().loadingFiles);
        assertNotNull(source.getLoadStatus().lastLoadedTime);
    }

    @Test
    public void testTransientTaskStateIsRebuiltWithoutJournalWrites() throws Exception {
        Pipe source = pipe(1, Pipe.State.RUNNING);
        Map<Long, PipeTaskDesc> tasks = getField(source, "runningTasks");
        tasks.put(9L, mock(PipeTaskDesc.class));
        setField(source, "recovered", true);
        setField(source, "failedTaskExecutionCount", 8);
        source.setLastPolledTime(1234);
        source.getLastErrorInfo().errorMessage = "previous leader error";
        source.getLastErrorInfo().errorTime = LocalDateTime.now();
        Pipe restored = Pipe.fromProto(source.toProto());
        assertTrue(restored.getRunningTasks().isEmpty());
        assertFalse(restored.isRecovered());
        assertEquals(0, restored.getFailedTaskExecutionCount());
        assertEquals(0, restored.getLastPolledTime());
        assertNull(restored.getLastErrorInfo().errorMessage);
        assertNull(restored.getLastErrorInfo().errorTime);
        assertNotSame(source.getLastErrorInfo(), restored.getLastErrorInfo());
        assertNotSame(getField(source, "lock"), getField(restored, "lock"));
        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testFileRepoRebuiltAndBoundToRestoredParentPipeId() throws Exception {
        Pipe source = pipe(1, Pipe.State.SUSPEND);
        PipePB pb = source.toProto().toBuilder().setPipeSource(source.toProto().getPipeSource().toBuilder()
                .setPipeId(new PipeId(999, 888).toProto())).build();
        Pipe restored = Pipe.fromProto(pb);
        FileListRepo repo = restored.getPipeSource().getFileListRepo();
        assertNotNull(repo);
        assertNotSame(source.getPipeSource().getFileListRepo(), repo);
        assertEquals(new PipeId(8, 1), getField(repo, "pipeId"));
        assertEquals(new PipeId(8, 1), getField(restored.getPipeSource(), "pipeId"));
        assertEquals("s3://bucket/file", restored.getPipeSource().getPath());
        assertEquals("parquet", restored.getPipeSource().getFormat());
    }

    @Test
    public void testPropertiesOverridePersistedSourceAndTaskDefaultsAsV2() throws Exception {
        PipePB pb = pipe(1, Pipe.State.RUNNING).toProto();
        pb = pb.toBuilder().setPipeSource(pb.getPipeSource().toBuilder().setAutoIngest(true)
                .setBatchSize(1).setBatchFiles(2)).build();
        Pipe restored = Pipe.fromProto(pb);
        assertFalse(restored.getPipeSource().getAutoIngest());
        assertEquals(1048576L, restored.getPipeSource().getBatchSize());
        assertEquals(7L, restored.getPipeSource().getBatchFiles());
        assertEquals("123", restored.getTaskProperties().get("query_timeout"));
        restored.processProperties(Map.of("poll_interval", "300", "task.query_timeout", "456"));
        assertEquals(300, restored.getPollIntervalSecond());
        assertEquals("456", restored.getTaskProperties().get("query_timeout"));
        restored.getPipeSource().getTableProperties().put("new_property", "new_value");
        assertEquals("new_value", restored.getPipeSource().getTableProperties().get("new_property"));
    }

    @Test
    public void testEmptyPropertiesPreserveV2RecoveryDefaults() throws Exception {
        Pipe source = pipe(1, Pipe.State.RUNNING);
        source.getProperties().clear();
        Pipe expected = FinalManagersImageTestSupport.gson(source, Pipe.class);
        Pipe restored = Pipe.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(expected, restored);
        assertEquals(expected.getPollIntervalSecond(), restored.getPollIntervalSecond());
        assertEquals(0, restored.getPollIntervalSecond());
        assertEquals(1048576L, restored.getPipeSource().getBatchSize());
    }

    @Test
    public void testRecoveryReenablesSchedulingOnlyForRunningPipes() throws Exception {
        for (Pipe.State state : Pipe.State.values()) {
            Pipe restored = Pipe.fromProto(pipe(1, state).toProto());
            FileListRepo repo = mock(FileListRepo.class);
            when(repo.listFilesByState(FileListRepo.PipeFileState.LOADING, -1)).thenReturn(List.of());
            setField(restored.getPipeSource(), "fileListRepo", repo);
            assertFalse(restored.isRunnable());
            restored.recovery();
            assertTrue(restored.isRecovered());
            assertEquals(state == Pipe.State.RUNNING, restored.isRunnable());
            restored.recovery();
            verify(repo).listFilesByState(FileListRepo.PipeFileState.LOADING, -1);
            restored.resetForLeaderHandoff();
            assertFalse(restored.isRecovered());
        }
    }

    @Test
    public void testFileSourceNullEmptyAndZeroValues() throws Exception {
        FilePipeSource source = new FilePipeSource("", "", new HashMap<>());
        source.initPipeId(new PipeId(0, 0));
        source.setBatchFiles(0);
        source.setBatchSize(0);
        source.setAutoIngest(false);
        FilePipeSource restored = FilePipeSource.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(source, restored);
        assertEquals(0, restored.getBatchFiles());
        assertEquals(0, restored.getBatchSize());
        assertFalse(restored.getAutoIngest());
        restored.getTableProperties().put("key", "value");
        assertTrue(source.getTableProperties().isEmpty());
        FilePipeSource absent = FilePipeSource.fromProto(FilePipeSourcePB.getDefaultInstance());
        assertNull(absent.getPath());
        assertNull(absent.getFormat());
        assertNull(absent.getTableProperties());
        assertNull(getField(absent.getFileListRepo(), "pipeId"));
    }

    @Test
    public void testUnknownStateAndInvalidPropertiesFailInsteadOfInventingDefaults() {
        PipePB pb = pipe(1, Pipe.State.RUNNING).toProto();
        assertThrows(IllegalArgumentException.class, () -> Pipe.fromProto(pb.toBuilder().setStateValue(900).build()));
        assertThrows(IllegalArgumentException.class, () -> Pipe.fromProto(pb.toBuilder().setTypeValue(901).build()));
        assertThrows(IllegalArgumentException.class, () -> Pipe.fromProto(pb.toBuilder()
                .setProperties(StringMapPB.newBuilder().putValues("unsupported", "value")).build()));
        assertThrows(NumberFormatException.class, () -> Pipe.fromProto(pb.toBuilder()
                .setProperties(StringMapPB.newBuilder().putValues("poll_interval", "bad")).build()));
    }

    @Test
    public void testIndependentPipesRemainSeparateEntriesAndNameIndexCanBeRemoved() throws Exception {
        PipeManager source = new PipeManager();
        for (int i = 1; i <= 1024; i++) {
            source.putPipe(pipe(i, Pipe.State.SUSPEND));
        }
        PipeManager restored = restore(source);
        assertEquals(1024, restored.getAllPipes().size());
        assertEquals(1024, restored.getAllPipesOfDb(8).size());
        Pipe last = restored.getPipeById(new PipeId(8, 1024));
        restored.removePipe(last);
        assertNull(restored.getPipeById(last.getPipeId()));
        Map<Pair<Long, String>, PipeId> names = getField(restored, "nameToId");
        assertFalse(names.containsKey(last.getDbAndName()));
        assertEquals(1023, names.size());

    }

    @Test
    public void testEmptyImageRetainsExistingPipesLikeV2() throws Exception {
        PipeManager restored = new PipeManager();
        restored.putPipe(pipe(1, Pipe.State.SUSPEND));
        Path path = LoadImageTestSupport.save(tempDir, new PipeManager().getRepo()::save);
        LoadImageTestSupport.load(path, restored.getRepo().getImageV3Loaders());
        assertEquals(1, restored.getAllPipes().size());
        assertEquals("pipe-1", restored.getPipeById(new PipeId(8, 1)).getName());

    }
}
