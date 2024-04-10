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

package com.starrocks.load.pipe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.load.pipe.filelist.FileListTableRepo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilePipeSource implements GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(FilePipeSource.class);

    @SerializedName(value = "pipe_id")
    private PipeId pipeId;
    @SerializedName(value = "path")
    private String path;
    @SerializedName(value = "format")
    private String format;
    @SerializedName(value = "table_properties")
    private Map<String, String> tableProperties;
    @SerializedName(value = "auto_ingest")
    private boolean autoIngest = true;
    @SerializedName(value = "batch_size")
    private long batchSize = Pipe.DEFAULT_BATCH_SIZE;
    @SerializedName(value = "batch_files")
    private long batchFiles = Pipe.DEFAULT_BATCH_FILES;
    @SerializedName(value = "eos")
    private boolean eos = false;

    private FileListRepo fileListRepo;

    public FilePipeSource(String path, String format, Map<String, String> sourceProperties) {
        this.path = Preconditions.checkNotNull(path);
        this.format = Preconditions.checkNotNull(format);
        this.tableProperties = Preconditions.checkNotNull(sourceProperties);
        this.fileListRepo = FileListRepo.createTableBasedRepo();
    }

    public void initPipeId(PipeId pipeId) {
        this.pipeId = pipeId;
        this.fileListRepo.setPipeId(pipeId);
    }

    public void poll() {
        if (eos) {
            return;
        }
        if (CollectionUtils.isEmpty(fileListRepo.listFilesByState(FileListRepo.PipeFileState.UNLOADED, 1))) {
            BrokerDesc brokerDesc = new BrokerDesc(tableProperties);
            try {
                List<FileStatus> files = HdfsUtil.listFileMeta(path, brokerDesc);
                List<PipeFileRecord> records =
                        ListUtils.emptyIfNull(files).stream()
                                .map(PipeFileRecord::fromHdfsFile)
                                .collect(Collectors.toList());
                fileListRepo.stageFiles(records);

                if (!autoIngest) {
                    // TODO: persist state
                    eos = true;
                }
            } catch (UserException e) {
                LOG.error("Failed to poll the source: ", e);
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("Failed to poll the source", e);
                throw e;
            }
        }
    }

    /**
     * For one-shot pipe, it will reach the eos state, which mean no more data from source
     * For continuous pipe, it will never reach the eos state
     */
    public boolean eos() {
        return eos;
    }

    public boolean allLoaded() {
        List<PipeFileRecord> errorFiles = fileListRepo.listFilesByState(FileListRepo.PipeFileState.ERROR, 1);
        if (CollectionUtils.isNotEmpty(errorFiles)) {
            return false;
        }
        List<PipeFileRecord> unloadedFiles = fileListRepo.listFilesByState(FileListRepo.PipeFileState.UNLOADED, 1);
        return CollectionUtils.isEmpty(unloadedFiles);
    }

    /**
     * Build a piece with size limitation and files limitation
     */
    public FilePipePiece pullPiece() {
        Preconditions.checkArgument(batchSize > 0, "not support batch_size=0");

        List<PipeFileRecord> unloadFiles = fileListRepo.listFilesByState(FileListRepo.PipeFileState.UNLOADED,
                batchFiles);
        if (CollectionUtils.isEmpty(unloadFiles)) {
            return null;
        }
        FilePipePiece piece = new FilePipePiece();
        long totalBytes = 0;
        for (PipeFileRecord file : ListUtils.emptyIfNull(unloadFiles)) {
            totalBytes += file.getFileSize();
            piece.addFile(file);
            if (totalBytes >= batchSize) {
                break;
            }
        }

        return piece;
    }

    public void finishPiece(PipeTaskDesc taskDesc) {
        FilePipePiece piece = taskDesc.getPiece();
        PipeTaskDesc.PipeTaskState taskState = taskDesc.getState();
        FileListRepo.PipeFileState state = taskState == PipeTaskDesc.PipeTaskState.ERROR ?
                FileListRepo.PipeFileState.ERROR : FileListRepo.PipeFileState.FINISHED;
        // TODO: distinguish file granular error message
        String errorMsg = taskDesc.getErrorMsg();
        piece.getFiles().forEach(file -> file.errorMessage = errorMsg);
        fileListRepo.updateFileState(piece.getFiles(), state, null);
    }

    public void retryErrorFiles() {
        List<PipeFileRecord> errorFiles = fileListRepo.listFilesByState(FileListRepo.PipeFileState.ERROR, 0);
        if (CollectionUtils.isNotEmpty(errorFiles)) {
            for (List<PipeFileRecord> batch : ListUtils.partition(errorFiles, FileListTableRepo.SELECT_BATCH_SIZE)) {
                fileListRepo.updateFileState(batch, FileListRepo.PipeFileState.UNLOADED, null);
                LOG.info("pipe {} retry error files: {}", pipeId, errorFiles);
            }
        }
    }

    public void retryFailedFile(String fileName) {
        PipeFileRecord record = fileListRepo.listFilesByPath(fileName);
        if (record != null && record.getLoadState().equals(FileListRepo.PipeFileState.ERROR)) {
            fileListRepo.updateFileState(Lists.newArrayList(record), FileListRepo.PipeFileState.UNLOADED, null);
            LOG.info("pipe {} retry error files: {}", pipeId, record);
        }
    }

    public void setAutoIngest(boolean autoIngest) {
        this.autoIngest = autoIngest;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void setBatchFiles(long batchFiles) {
        this.batchFiles = batchFiles;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setPipeId(PipeId id) {
        this.pipeId = id;
    }

    public String getPath() {
        return path;
    }

    public String getFormat() {
        return format;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public FileListRepo getFileListRepo() {
        return fileListRepo;
    }

    /**
     * Build insert sql from original pipe statement
     * Example: original sql: insert into tbl select * from files('path'='xxx')
     */
    public static String buildInsertSql(Pipe pipe, FilePipePiece piece, String label) {
        String originalSql = pipe.getOriginSql();
        StatementBase sqlStmt = SqlParser.parse(originalSql, new SessionVariable()).get(0);
        sqlStmt.setOrigStmt(new OriginStatement(originalSql, 0));
        Preconditions.checkState(sqlStmt instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) sqlStmt;
        SelectRelation select = (SelectRelation) insertStmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation tableFunctionRelation = (FileTableFunctionRelation) select.getRelation();

        // replace with a new Files table function
        Map<String, String> properties = new HashMap<>(tableFunctionRelation.getProperties());
        String files =
                piece.getFiles().stream().map(PipeFileRecord::getFileName).collect(Collectors.joining(","));
        properties.put(TableFunctionTable.PROPERTY_PATH, files);
        // replace with an insert label
        insertStmt.setLabel(label);

        FileTableFunctionRelation fileRelation = new FileTableFunctionRelation(properties, NodePosition.ZERO);
        select.setRelation(fileRelation);
        return AstToSQLBuilder.toSQL(sqlStmt);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.fileListRepo = FileListRepo.createTableBasedRepo();
        this.fileListRepo.setPipeId(pipeId);
    }

    @Override
    public String toString() {
        return "FILE_SOURCE(path=" + path + ")";
    }
}
