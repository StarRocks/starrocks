// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
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
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01')",
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
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01')",
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
                        "'2023-07-01 01:01:01', '2023-07-01 01:01:01')",
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
        String sql = FileListTableRepo.RepoAccessor.getInstance().buildSqlStartLoad(records, state);
        Assert.assertEquals("UPDATE _statistics_.pipe_file_list " +
                "SET state = 'LOADING', start_load = now() " +
                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf') " +
                "OR (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf')", sql);

        // finish load
        state = FileListRepo.PipeFileState.LOADED;
        sql = FileListTableRepo.RepoAccessor.getInstance().buildSqlFinishLoad(records, state);
        Assert.assertEquals("UPDATE _statistics_.pipe_file_list " +
                "SET state = 'LOADED', finish_load = now() " +
                "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf') " +
                "OR (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf')", sql);

        // add files
        sql = FileListTableRepo.RepoAccessor.getInstance().buildSqlAddFiles(records);
        Assert.assertEquals("INSERT INTO _statistics_.pipe_file_list VALUES " +
                "(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', '2023-07-01 01:01:01', " +
                "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '2023-07-01 01:01:01')," +
                "(1, 'a.parquet', '123asdf', 1024, 'UNLOADED', '2023-07-01 01:01:01', " +
                "'2023-07-01 01:01:01', '2023-07-01 01:01:01', '2023-07-01 01:01:01')", sql);

        // delete pipe
        sql = FileListTableRepo.RepoAccessor.getInstance().buildDeleteByPipe(1);
        Assert.assertEquals("DELETE FROM _statistics_.pipe_file_list WHERE pipe_id = 1", sql);

        // list unloaded files
        sql = FileListTableRepo.RepoAccessor.getInstance().buildListUnloadedFile(1);
        Assert.assertEquals("SELECT pipe_id, file_name, file_version, file_size, state, last_modified, " +
                "staged_time, start_load, finish_load FROM _statistics_.pipe_file_list " +
                "WHERE pipe_id = 1 AND state = 'UNLOADED'", sql);

        // select staged
        sql = FileListTableRepo.RepoAccessor.getInstance().buildSelectStagedFiles(records);
        Assert.assertEquals("SELECT pipe_id, file_name, file_version, file_size, state, last_modified, " +
                        "staged_time, start_load, finish_load FROM _statistics_.pipe_file_list " +
                        "WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf') " +
                        "OR (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '123asdf')",
                sql);
    }

    private void mockExecutor() {
        new MockUp<FileListTableRepo.RepoExecutor>() {
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
    public void testCreator() throws RuntimeException {
        mockExecutor();
        new MockUp<FileListTableRepo.RepoCreator>() {
            @Mock
            public boolean checkDatabaseExists() {
                return true;
            }
        };
        FileListTableRepo.RepoExecutor executor = FileListTableRepo.RepoExecutor.getInstance();
        FileListTableRepo.RepoCreator creator = FileListTableRepo.RepoCreator.getInstance();

        // failed for the first time
        new MockUp<FileListTableRepo.RepoExecutor>() {
            @Mock
            public void executeDDL(String sql) {
                throw new RuntimeException("ddl failed");
            }
        };
        creator.run();
        Assert.assertTrue(creator.isDatabaseExists());
        Assert.assertFalse(creator.isTableExists());
        Assert.assertFalse(creator.isTableCorrected());

        // succeed
        // failed for the first time
        new MockUp<FileListTableRepo.RepoExecutor>() {
            @Mock
            public void executeDDL(String sql) {
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
        FileListTableRepo.RepoAccessor accessor = FileListTableRepo.RepoAccessor.getInstance();
        FileListTableRepo.RepoExecutor executor = FileListTableRepo.RepoExecutor.getInstance();
        new Expectations(executor) {{
            executor.executeDQL(anyString);
            result = Lists.newArrayList();

            executor.executeDML(anyString);
            result = Lists.newArrayList();
        }};
        // listAllFiles
        Assert.assertTrue(accessor.listAllFiles().isEmpty());

        // listUnloadedFiles
        Assert.assertTrue(repo.listUnloadedFiles().isEmpty());
        Assert.assertTrue(accessor.listUnloadedFiles(1).isEmpty());

        // selectStagedFiles
        PipeFileRecord record = new PipeFileRecord();
        record.pipeId = 1;
        record.fileName = "a.parquet";
        record.fileVersion = "1";
        record.loadState = FileListRepo.PipeFileState.UNLOADED;
        Assert.assertTrue(accessor.selectStagedFiles(Lists.newArrayList(record)).isEmpty());

        // addFiles
        new Expectations(executor) {{
            executor.executeDML(
                    "INSERT INTO _statistics_.pipe_file_list VALUES (1, 'a.parquet', '1', 0, 'UNLOADED', NULL, NULL, NULL, NULL)");
            result = Lists.newArrayList();
        }};
        repo.addFiles(Lists.newArrayList(record));

        // updateFileState
        new Expectations(executor) {{
            executor.executeDML(
                    "UPDATE _statistics_.pipe_file_list SET state = 'LOADING', start_load = now() WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
            result = Lists.newArrayList();
        }};
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.LOADING);
        new Expectations(executor) {{
            executor.executeDML(
                    "UPDATE _statistics_.pipe_file_list SET state = 'LOADED', finish_load = now() WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
            result = Lists.newArrayList();
        }};
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.LOADED);
        new Expectations(executor) {{
            executor.executeDML(
                    "UPDATE _statistics_.pipe_file_list SET state = 'ERROR' WHERE (pipe_id = 1 AND file_name = 'a.parquet' AND file_version = '1')");
            result = Lists.newArrayList();
        }};
        accessor.updateFilesState(Lists.newArrayList(record), FileListRepo.PipeFileState.ERROR);
        repo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.ERROR);

        // delete by pipe
        new Expectations(executor) {{
            executor.executeDML("DELETE FROM _statistics_.pipe_file_list WHERE pipe_id = 1");
            result = Lists.newArrayList();
        }};
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

        FileListTableRepo.RepoExecutor executor = FileListTableRepo.RepoExecutor.getInstance();

        Assert.assertTrue(executor.executeDQL("select now()").isEmpty());

        Assert.assertThrows(SemanticException.class, () -> executor.executeDML("insert into a.b values (1) "));

        Assert.assertThrows(RuntimeException.class, () -> executor.executeDDL("create table a (id int) "));
    }
}
