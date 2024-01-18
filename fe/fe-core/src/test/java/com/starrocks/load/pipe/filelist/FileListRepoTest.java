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

package com.starrocks.load.pipe.filelist;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.structure.Pair;
import com.starrocks.common.structure.Status;
import com.starrocks.common.util.DateUtils;
import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResultBatch;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileListRepoTest {

    @Test
    public void testTestFileRecord() {
        long lastModified = 191231231231L;
        FileStatus hdfsFile = new FileStatus(
                1, false, 2, 3, lastModified,
                new Path("/a.parquet")
        );
        PipeFileRecord record = PipeFileRecord.fromHdfsFile(hdfsFile);
        record.pipeId = 1;

        String now = DateUtils.formatDateTimeUnix(LocalDateTime.now());
        Assert.assertEquals("/a.parquet", record.getFileName());
        Assert.assertEquals(1, record.getFileSize());
        Assert.assertEquals("191231231231", record.getFileVersion());
        Assert.assertEquals(now, DateUtils.formatDateTimeUnix(record.getStagedTime()));
        Assert.assertEquals(FileListRepo.PipeFileState.UNLOADED, record.getLoadState());

        // equals
        PipeFileRecord identifier = new PipeFileRecord();
        identifier.pipeId = 1;
        identifier.fileName = "/a.parquet";
        identifier.fileVersion = String.valueOf(lastModified);
        Assert.assertEquals(identifier, record);
        Set<PipeFileRecord> records = Sets.newHashSet(record);
        Assert.assertTrue(records.contains(identifier));
        System.out.println(records);
    }

    @Test
    public void testPipeFileRecord() {
        String json = "{\"data\": [1, \"a.parquet\", \"123asdf\", 1024, \"UNLOADED\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        PipeFileRecord record = PipeFileRecord.fromJson(json);
        String valueList = record.toValueList();
        Assert.assertEquals("(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '{\"errorMessage\":null}', '')",
                valueList);

        // contains empty value
        json = "{\"data\": [1, \"a.parquet\", \"\", 1024, \"UNLOADED\", " +
                "\"\", \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        record = PipeFileRecord.fromJson(json);
        valueList = record.toValueList();
        Assert.assertEquals("(1, 'a.parquet', '', 1024, 'UNLOADED', " +
                        "NULL, '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '{\"errorMessage\":null}', '')",
                valueList);

        // contains null value
        json = "{\"data\": [1, \"a.parquet\", \"\", 1024, \"UNLOADED\", " +
                "null, \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        record = PipeFileRecord.fromJson(json);
        valueList = record.toValueList();
        Assert.assertEquals("(1, 'a.parquet', '', 1024, 'UNLOADED', " +
                        "NULL, '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '{\"errorMessage\":null}', '')",
                valueList);
    }

    @Test
    public void testSqlBuilder() {
        // update state
        String json = "{\"data\": [1, \"a.parquet\", \"123asdf\", 1024, \"UNLOADED\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\", " +
                "\"2023-07-01 01:01:01\", \"2023-07-01 01:01:01\" " +
                "]}";
        List<PipeFileRecord> records =
                Arrays.asList(PipeFileRecord.fromJson(json), PipeFileRecord.fromJson(json));
        FileListRepo.PipeFileState state = FileListRepo.PipeFileState.LOADING;
        String sql = RepoAccessor.getInstance().buildSqlStartLoad(records, state, "insert-label");
        Assert.assertEquals("UPDATE _statistics_.pipe_file_list " +
                "SET `state` = 'LOADING', `start_load` = now(), `insert_label`='insert-label' " +
                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf') " +
                "OR (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf')", sql);

        // finish load
        state = FileListRepo.PipeFileState.FINISHED;
        sql = RepoAccessor.getInstance().buildSqlFinishLoad(records, state);
        Assert.assertEquals("UPDATE _statistics_.pipe_file_list " +
                "SET `state` = 'FINISHED', `finish_load` = now() " +
                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf') " +
                "OR (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf')", sql);

        // add files
        sql = RepoAccessor.getInstance().buildSqlAddFiles(records);
        Assert.assertEquals("INSERT INTO _statistics_.pipe_file_list" +
                        "(`pipe_id`, `file_name`, `file_version`, `file_size`, `state`, `last_modified`, " +
                        "`staged_time`, `start_load`, `finish_load`, `error_info`, `insert_label`) VALUES " +
                "(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '2023-07-01 01:01:01', '{\"errorMessage\":null}', '')," +
                "(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', '2023-07-01 01:01:01', " +
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '2023-07-01 01:01:01', '{\"errorMessage\":null}', '')",
                sql);

        // delete pipe
        sql = RepoAccessor.getInstance().buildDeleteByPipe(1);
        Assert.assertEquals("DELETE FROM _statistics_.pipe_file_list WHERE `pipe_id` = 1", sql);

        // list unloaded files
        sql = RepoAccessor.getInstance().buildListFileByState(1, FileListRepo.PipeFileState.UNLOADED, 0);
        Assert.assertEquals("SELECT `pipe_id`, `file_name`, `file_version`, `file_size`, `state`, " +
                "`last_modified`, `staged_time`, `start_load`, `finish_load`, `error_info`, `insert_label` " +
                "FROM _statistics_.pipe_file_list WHERE `pipe_id` = 1 AND `state` = 'UNLOADED'", sql);

        // listFilesByPath
        sql = RepoAccessor.getInstance().buildListFileByPath(1, "file1.parquet");
        Assert.assertEquals("SELECT `pipe_id`, `file_name`, `file_version`, `file_size`, `state`, " +
                "`last_modified`, `staged_time`, `start_load`, `finish_load`, `error_info`, `insert_label` " +
                "FROM _statistics_.pipe_file_list WHERE `pipe_id` = 1 AND `file_name` = 'file1.parquet'", sql);

        // select staged
        sql = RepoAccessor.getInstance().buildSelectStagedFiles(records);
        Assert.assertEquals("SELECT `pipe_id`, `file_name`, `file_version`, `file_size`, `state`, " +
                "`last_modified`, `staged_time`, `start_load`, `finish_load`, `error_info`, `insert_label` " +
                "FROM _statistics_.pipe_file_list WHERE (pipe_id = 1 AND file_name = 'a.parquet' " +
                "AND file_version = '123asdf') OR (pipe_id = 1 AND file_name = 'a.parquet' " +
                "AND file_version = '123asdf')", sql);
    }

    private void mockExecutor() {
        new MockUp<RepoExecutor>() {
            private boolean ddlExecuted = false;

            @Mock
            public void executeDML(String sql) {
            }

            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                return Lists.newArrayList();
            }

            @Mock
            public void executeDDL(String sql) {
                if (!ddlExecuted) {
                    throw new RuntimeException("ddl failed");
                }
                ddlExecuted = true;
            }
        };
    }

    @Test
    public void testCreator() throws RuntimeException, UserException {
        mockExecutor();
        new MockUp<RepoCreator>() {
            @Mock
            public boolean checkDatabaseExists() {
                return true;
            }
        };
        RepoExecutor executor = RepoExecutor.getInstance();
        RepoCreator creator = RepoCreator.getInstance();

        // failed for the first time
        new MockUp<RepoExecutor>() {
            @Mock
            public void executeDDL(String sql) {
                throw new RuntimeException("ddl failed");
            }
        };
        creator.run();
        Assert.assertTrue(creator.isDatabaseExists());
        Assert.assertFalse(creator.isTableExists());
        Assert.assertFalse(creator.isTableCorrected());

        // create with 1 replica
        new MockUp<SystemInfoService>() {
            @Mock
            public int getTotalBackendNumber() {
                return 1;
            }
        };
        new MockUp<RepoExecutor>() {
            @Mock
            public void executeDDL(String sql) {
            }
        };
        creator.run();
        Assert.assertTrue(creator.isTableExists());
        Assert.assertFalse(creator.isTableCorrected());

        // be corrected to 3 replicas
        new MockUp<SystemInfoService>() {
            @Mock
            public int getTotalBackendNumber() {
                return 3;
            }
        };

        creator.run();
        Assert.assertTrue(creator.isDatabaseExists());
        Assert.assertTrue(creator.isTableExists());
        Assert.assertTrue(creator.isTableCorrected());
    }

    @Test
    public void testRepo() {
        FileListTableRepo repo = new FileListTableRepo();
        repo.setPipeId(new PipeId(1, 1));
        RepoAccessor accessor = RepoAccessor.getInstance();
        RepoExecutor executor = RepoExecutor.getInstance();
        new Expectations(executor) {
            {
                executor.executeDQL(anyString);
                result = Lists.newArrayList();

                executor.executeDML(anyString);
                result = Lists.newArrayList();
            }
        };
        // listAllFiles
        Assert.assertTrue(accessor.listAllFiles().isEmpty());

        // listUnloadedFiles
        Assert.assertTrue(repo.listFilesByState(FileListRepo.PipeFileState.UNLOADED, 0).isEmpty());
        Assert.assertTrue(accessor.listFilesByState(1, FileListRepo.PipeFileState.UNLOADED, 0).isEmpty());

        // listFileByPath
        Assert.assertThrows(IllegalArgumentException.class, () -> repo.listFilesByPath("not-exists"));
        Assert.assertThrows(IllegalArgumentException.class, () -> accessor.listFilesByPath(1, "not-exists"));

        // selectStagedFiles
        PipeFileRecord record = new PipeFileRecord();
        record.pipeId = 1;
        record.fileName = "a.parquet";
        record.fileVersion = "1";
        record.loadState = FileListRepo.PipeFileState.UNLOADED;
        Assert.assertTrue(accessor.selectStagedFiles(Lists.newArrayList(record)).isEmpty());

        // addFiles
        new Expectations(executor) {
            {
                executor.executeDML(
                        "INSERT INTO _statistics_.pipe_file_list(`pipe_id`, `file_name`, `file_version`, " +
                                "`file_size`, `state`, `last_modified`, `staged_time`, `start_load`, `finish_load`, " +
                                "`error_info`, `insert_label`) VALUES " +
                                "(1, 'a.parquet', '1', 0, 'UNLOADED', NULL, NULL, " +
                                "NULL, NULL, '{\"errorMessage\":null}', '')");
                result = Lists.newArrayList();
            }
        };
        repo.stageFiles(Lists.newArrayList(record));

        // updateFileState
        new Expectations(executor) {
            {
                executor.executeDML(
                        "UPDATE _statistics_.pipe_file_list " +
                                "SET `state` = 'LOADING', `start_load` = now(), `insert_label`='insert-label' " +
                                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
                result = Lists.newArrayList();
            }
        };
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.LOADING, "insert-label");
        new Expectations(executor) {
            {
                executor.executeDML(
                        "UPDATE _statistics_.pipe_file_list " +
                                "SET `state` = 'FINISHED', `finish_load` = now() " +
                                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
                result = Lists.newArrayList();
            }
        };
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.FINISHED, null);
        new Expectations(executor) {
            {
                executor.executeDML(
                        "UPDATE _statistics_.pipe_file_list " +
                                "SET `state` = 'ERROR', `error_info` = '' " +
                                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
                result = Lists.newArrayList();
            }
        };
        accessor.updateFilesState(Lists.newArrayList(record), FileListRepo.PipeFileState.ERROR, null);
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.ERROR, null);

        // delete by pipe
        new Expectations(executor) {
            {
                executor.executeDML("DELETE FROM _statistics_.pipe_file_list WHERE `pipe_id` = 1");
                result = Lists.newArrayList();
            }
        };
        repo.destroy();
    }

    @Test
    public void testExecutor(@Mocked StmtExecutor stmtExecutor) throws IOException {
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                return new Pair<>(Lists.newArrayList(), new Status());
            }

            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };

        RepoExecutor executor = RepoExecutor.getInstance();

        Assert.assertTrue(executor.executeDQL("select now()").isEmpty());

        Assert.assertThrows(SemanticException.class, () -> executor.executeDML("insert into a.b values (1) "));

        Assert.assertThrows(RuntimeException.class, () -> executor.executeDDL("create table a (id int) "));
    }

    @Test
    @Ignore("jvm crash FIXME(murphy)")
    public void testDMLException() throws Exception {
        FileListTableRepo repo = new FileListTableRepo();
        repo.setPipeId(new PipeId(1, 1));
        RepoAccessor accessor = RepoAccessor.getInstance();
        RepoExecutor executor = RepoExecutor.getInstance();

        new Expectations(executor) {
            {
                executor.executeDQL(anyString);
                result = Lists.newArrayList();
                times = 101;

                executor.executeDML(anyString);
                result = Lists.newArrayList();
                times = 2;
            }
        };
        // stage a large batch of files
        List<PipeFileRecord> files = new ArrayList<>();
        for (int i = 0; i < FileListTableRepo.WRITE_BATCH_SIZE + 1; i++) {
            PipeFileRecord record = new PipeFileRecord();
            record.pipeId = 1;
            record.fileName = String.format("%d.parquet", i);
            record.fileVersion = "1";
            record.loadState = FileListRepo.PipeFileState.UNLOADED;
            files.add(record);
        }
        repo.stageFiles(files);

        // stage files error
        new Expectations(executor) {
            {
                executor.executeDQL(anyString);
                result = Lists.newArrayList();
                times = 100;

                executor.executeDML(anyString);
                result = new Exception("too many versions");
            }
        };
        try {
            repo.stageFiles(files);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("too many versions", e.getMessage());
        }
    }
}
