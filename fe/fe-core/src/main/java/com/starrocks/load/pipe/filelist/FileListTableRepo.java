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

import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo extends FileListRepo {

    private static final Logger LOG = LogManager.getLogger(FileListTableRepo.class);

    protected static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    protected static final String FILE_LIST_TABLE_NAME = "pipe_file_list";
    protected static final String FILE_LIST_FULL_NAME = FILE_LIST_DB_NAME + "." + FILE_LIST_TABLE_NAME;

    // Split the records into batches, to avoid build a tremendous SQL
    public static final int SELECT_BATCH_SIZE = 20;
    public static final int WRITE_BATCH_SIZE = 2000;

    // NOTE: why not use the (pipe_id, file_name, file_version) as primary key since it's unique
    // Because current primary key implementation limit the length to 128
    protected static final String FILE_LIST_TABLE_CREATE =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "id bigint not null auto_increment, " +
                    "pipe_id bigint, " +
                    "file_name string, " +
                    "file_version string, " +
                    "file_size bigint, " +
                    "state string, " +
                    "last_modified  datetime, " +
                    "staged_time datetime, " +
                    "start_load datetime, " +
                    "finish_load datetime, " +
                    "error_info string, " +
                    "insert_label string" +
                    " ) PRIMARY KEY(id) " +
                    "DISTRIBUTED BY HASH(id) BUCKETS 8 " +
                    "ORDER BY (pipe_id, file_name) " +
                    "properties('replication_num' = '%d') ";

    protected static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    protected static final String ALL_COLUMNS =
            "`pipe_id`, `file_name`, `file_version`, `file_size`, `state`, `last_modified`, `staged_time`," +
                    " `start_load`, `finish_load`, `error_info`, `insert_label`";

    protected static final String SELECT_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME;

    protected static final String SELECT_FILES_BY_STATE = SELECT_FILES + " WHERE `pipe_id` = %d AND `state` = %s";

    protected static final String SELECT_FILES_BY_PATH = SELECT_FILES + " WHERE `pipe_id` = %d AND `file_name` = %s";

    protected static final String SELECT_FILES_BY_STATE_WITH_LIMIT =
            SELECT_FILES + " WHERE `pipe_id` = %d AND `state` = %s LIMIT %d";

    protected static final String UPDATE_FILE_STATE =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `error_info` = %s WHERE ";

    protected static final String UPDATE_FILE_STATE_START_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `start_load` = now(), `insert_label`=%s WHERE ";

    protected static final String UPDATE_FILE_STATE_FINISH_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET `state` = %s, `finish_load` = now() WHERE ";

    protected static final String INSERT_FILES =
            "INSERT INTO " + FILE_LIST_FULL_NAME + "(" + ALL_COLUMNS + ")" + " VALUES ";

    protected static final String SELECTED_STAGED_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME + " WHERE ";

    protected static final String DELETE_BY_PIPE = "DELETE FROM " + FILE_LIST_FULL_NAME + " WHERE `pipe_id` = %d";

    @Override
    public List<PipeFileRecord> listFilesByState(PipeFileState state, long limit) {
        return RepoAccessor.getInstance().listFilesByState(pipeId.getId(), state, limit);
    }

    @Override
    public PipeFileRecord listFilesByPath(String path) {
        return RepoAccessor.getInstance().listFilesByPath(pipeId.getId(), path);
    }

    @Override
    public void stageFiles(List<PipeFileRecord> records) {
        records.forEach(file -> file.pipeId = pipeId.getId());

        List<PipeFileRecord> stagingFile = new ArrayList<>();
        for (List<PipeFileRecord> batch : ListUtils.partition(records, SELECT_BATCH_SIZE)) {
            List<PipeFileRecord> stagedFiles = RepoAccessor.getInstance().selectStagedFiles(batch);
            List<PipeFileRecord> newFiles = ListUtils.subtract(batch, stagedFiles);
            if (CollectionUtils.isEmpty(newFiles)) {
                return;
            }
            stagingFile.addAll(newFiles);

            if (stagingFile.size() >= WRITE_BATCH_SIZE) {
                RepoAccessor.getInstance().addFiles(stagingFile);
                LOG.info("stage {} files into file-list, pipe={}, newFiles={}",
                        stagingFile.size(), pipeId, stagingFile);
                stagingFile.clear();
            }
        }
        if (CollectionUtils.isNotEmpty(stagingFile)) {
            RepoAccessor.getInstance().addFiles(stagingFile);
            LOG.info("stage {} files into file-list, pipe={}, newFiles={}",
                    stagingFile.size(), pipeId, stagingFile);
        }
    }

    @Override
    public void updateFileState(List<PipeFileRecord> files, PipeFileState state, String insertLabel) {
        files.forEach(x -> x.pipeId = pipeId.getId());
        RepoAccessor.getInstance().updateFilesState(files, state, insertLabel);
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

        public static String buildCreateTableSql() throws UserException {
            int replica = AutoInferUtil.calDefaultReplicationNum();
            return String.format(FILE_LIST_TABLE_CREATE,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME), replica);
        }

        public static String buildAlterTableSql() {
            return String.format(CORRECT_FILE_LIST_REPLICATION_NUM,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME));
        }
    }

}