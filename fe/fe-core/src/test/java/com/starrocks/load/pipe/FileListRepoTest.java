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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FileListRepoTest {

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
}
