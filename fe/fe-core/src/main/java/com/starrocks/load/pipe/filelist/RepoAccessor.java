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

import com.google.common.base.Preconditions;
import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.thrift.TResultBatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Query the repo
 */
public class RepoAccessor {

    private static final Logger LOG = LogManager.getLogger(RepoAccessor.class);
    private static final RepoAccessor INSTANCE = new RepoAccessor();

    public static RepoAccessor getInstance() {
        return INSTANCE;
    }

    public List<PipeFileRecord> listAllFiles() {
        try {
            List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(
                    FileListTableRepo.SELECT_FILES);
            return PipeFileRecord.fromResultBatch(batch);
        } catch (Exception e) {
            LOG.error("listAllFiles failed", e);
            throw e;
        }
    }

    public List<PipeFileRecord> listUnloadedFiles(long pipeId) {
        List<PipeFileRecord> res = null;
        try {
            String sql = buildListUnloadedFile(pipeId);
            List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(sql);
            res = PipeFileRecord.fromResultBatch(batch);
        } catch (Exception e) {
            LOG.error("listUnloadedFiles failed", e);
            throw e;
        }
        return res;
    }

    public List<PipeFileRecord> selectStagedFiles(List<PipeFileRecord> records) {
        try {
            String sql = buildSelectStagedFiles(records);
            List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(sql);
            return PipeFileRecord.fromResultBatch(batch);
        } catch (Exception e) {
            LOG.error("selectStagedFiles failed", e);
            throw e;
        }
    }

    public void addFiles(List<PipeFileRecord> records) {
        try {
            String sql = buildSqlAddFiles(records);
            RepoExecutor.getInstance().executeDML(sql);
            LOG.info("addFiles into repo: {}", records);
        } catch (Exception e) {
            LOG.error("addFiles {} failed", records, e);
            throw e;
        }
    }

    /**
     * pipe_id, file_name, file_version are required to locate unique file
     */
    public void updateFilesState(List<PipeFileRecord> records, FileListRepo.PipeFileState state) {
        try {
            String sql = null;
            switch (state) {
                case UNLOADED:
                case ERROR:
                case SKIPPED:
                    sql = buildSqlUpdateState(records, state);
                    break;
                case LOADING:
                    sql = buildSqlStartLoad(records, state);
                    break;
                case LOADED:
                    sql = buildSqlFinishLoad(records, state);
                    break;
                default:
                    Preconditions.checkState(false, "not supported");
                    break;
            }
            RepoExecutor.getInstance().executeDML(sql);
            LOG.info("update files state to {}: {}", state, records);
        } catch (Exception e) {
            LOG.error("update files state failed: {}", records, e);
            throw e;
        }
    }

    public void deleteByPipe(long pipeId) {
        try {
            String sql = String.format(FileListTableRepo.DELETE_BY_PIPE, pipeId);
            RepoExecutor.getInstance().executeDML(sql);
            LOG.info("delete pipe files {}", pipeId);
        } catch (Exception e) {
            LOG.error("delete file of pipe {} failed", pipeId, e);
            throw e;
        }
    }

    protected String buildSelectStagedFiles(List<PipeFileRecord> files) {
        String where = files.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR "));
        return FileListTableRepo.SELECTED_STAGED_FILES + where;
    }

    protected String buildListUnloadedFile(long pipeId) {
        String sql = String.format(FileListTableRepo.SELECT_FILES_BY_STATE,
                pipeId, Strings.quote(FileListRepo.PipeFileState.UNLOADED.toString()));
        return sql;
    }

    protected String buildDeleteByPipe(long pipeId) {
        return String.format(FileListTableRepo.DELETE_BY_PIPE, pipeId);
    }

    protected String buildSqlAddFiles(List<PipeFileRecord> records) {
        StringBuilder sb = new StringBuilder();
        sb.append(FileListTableRepo.INSERT_FILES);
        sb.append(records.stream().map(PipeFileRecord::toValueList).collect(Collectors.joining(",")));
        return sb.toString();
    }

    protected String buildSqlUpdateState(List<PipeFileRecord> records, FileListRepo.PipeFileState state) {
        // FIXME: update error message for each file, use partial update capability
        String errorMessage = records.stream()
                .filter(x -> StringUtils.isNotEmpty(x.getErrorMessage()))
                .findFirst()
                .map(PipeFileRecord::toErrorInfo).orElse("");
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(FileListTableRepo.UPDATE_FILE_STATE,
                Strings.quote(state.toString()), Strings.quote(errorMessage)));
        sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
        return sb.toString();
    }

    public String buildSqlStartLoad(List<PipeFileRecord> records, FileListRepo.PipeFileState state) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(FileListTableRepo.UPDATE_FILE_STATE_START_LOAD, Strings.quote(state.toString())));
        sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
        return sb.toString();
    }

    protected String buildSqlFinishLoad(List<PipeFileRecord> records, FileListRepo.PipeFileState state) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(FileListTableRepo.UPDATE_FILE_STATE_FINISH_LOAD, Strings.quote(state.toString())));
        sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
        return sb.toString();
    }
}
