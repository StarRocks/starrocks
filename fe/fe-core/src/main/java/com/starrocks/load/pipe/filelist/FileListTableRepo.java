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

package com.starrocks.load.pipe.filelist;

import com.starrocks.catalog.CatalogUtils;
import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo extends FileListRepo {

    private static final Logger LOG = LogManager.getLogger(FileListTableRepo.class);

    protected static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    protected static final String FILE_LIST_TABLE_NAME = "pipe_file_list";
    protected static final String FILE_LIST_FULL_NAME = FILE_LIST_DB_NAME + "." + FILE_LIST_TABLE_NAME;

    protected static final String FILE_LIST_TABLE_CREATE =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "pipe_id bigint, " +
                    "file_name string, " +
                    "file_version string, " +
                    "file_size bigint, " +
                    "state string, " +
                    "last_modified  datetime, " +
                    "staged_time datetime, " +
                    "start_load datetime, " +
                    "finish_load datetime, " +
                    "error_info_json string " +
                    " ) PRIMARY KEY(pipe_id, file_name, file_version) " +
                    "DISTRIBUTED BY HASH(pipe_id, file_name) BUCKETS 8 " +
                    "properties('replication_num' = '%d') ";

    protected static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    protected static final String ALL_COLUMNS =
            "`pipe_id`, `file_name`, `file_version`, `file_size`, `state`, `last_modified`, `staged_time`," +
                    " `start_load`, `finish_load`, error_info_json";

    protected static final String SELECT_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME;

    protected static final String SELECT_FILES_BY_STATE = SELECT_FILES + " WHERE `pipe_id` = %d AND `state` = %s";

    protected static final String UPDATE_FILE_STATE =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `error_info_json` = %s WHERE ";

    protected static final String UPDATE_FILE_STATE_START_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `start_load` = now() WHERE ";

    protected static final String UPDATE_FILE_STATE_FINISH_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `finish_load` = now() WHERE ";

    protected static final String INSERT_FILES =
            "INSERT INTO " + FILE_LIST_FULL_NAME + " VALUES ";

    protected static final String SELECTED_STAGED_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME + " WHERE ";

    protected static final String DELETE_BY_PIPE = "DELETE FROM " + FILE_LIST_FULL_NAME + " WHERE `pipe_id` = %d";

    @Override
    public List<PipeFileRecord> listUnloadedFiles() {
        return RepoAccessor.getInstance().listUnloadedFiles(pipeId.getId());
    }

    @Override
    public void addFiles(List<PipeFileRecord> records) {
        records.forEach(file -> file.pipeId = pipeId.getId());
        List<PipeFileRecord> stagedFiles = RepoAccessor.getInstance().selectStagedFiles(records);
        List<PipeFileRecord> newFiles = ListUtils.subtract(records, stagedFiles);
        if (CollectionUtils.isEmpty(newFiles)) {
            return;
        }
        RepoAccessor.getInstance().addFiles(records);
        LOG.info("add files into file-list, pipe={}, alreadyStagedFile={}, newFiles={}", pipeId, stagedFiles, newFiles);
    }

    @Override
    public void updateFileState(List<PipeFileRecord> files, PipeFileState state) {
        files.forEach(x -> x.pipeId = pipeId.getId());
        RepoAccessor.getInstance().updateFilesState(files, state);
    }

    @Override
    public void cleanup() {
        // TODO
    }

    @Override
    public void destroy() {
        RepoAccessor.getInstance().deleteByPipe(pipeId.getId());
    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {

        public static String buildCreateTableSql() {
            int replica = Math.min(3, GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber());
            return String.format(FILE_LIST_TABLE_CREATE,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME), replica);
        }

        public static String buildAlterTableSql() {
            return String.format(CORRECT_FILE_LIST_REPLICATION_NUM,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME));
        }
    }

}