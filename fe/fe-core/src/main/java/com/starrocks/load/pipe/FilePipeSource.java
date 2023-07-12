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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    @SerializedName(value = "eos")
    private boolean eos = false;

    private FileListRepoInMemory fileListRepo;

    public FilePipeSource(String path, String format, Map<String, String> sourceProperties) {
        this.path = Preconditions.checkNotNull(path);
        this.format = Preconditions.checkNotNull(format);
        this.tableProperties = Preconditions.checkNotNull(sourceProperties);
        this.fileListRepo = new FileListRepoInMemory();
    }

    public void initPipeId(PipeId pipeId) {
        this.pipeId = pipeId;
        this.fileListRepo.set
    }

    public void poll() {
        // TODO: poll it seriously
        if (fileListRepo.size() == 0) {
            BrokerDesc brokerDesc = new BrokerDesc(tableProperties);
            List<TBrokerFileStatus> fileList = Lists.newArrayList();
            try {
                HdfsUtil.parseFile(path, brokerDesc, fileList);
            } catch (UserException e) {
                LOG.error("Failed to poll the source: ", e);
                throw new RuntimeException(e);
            } catch (Throwable e) {
                LOG.error("Failed to poll the source", e);
                throw e;
            }

            fileListRepo.addBrokerFiles(fileList);
        }
        if (!autoIngest) {
            // TODO: persist state
            eos = true;
        }
    }

    public boolean eos() {
        return eos;
    }

    public FilePipePiece pullPiece() {
        Preconditions.checkArgument(batchSize > 0, "not support batch_size=0");

        List<PipeFile> unloadFiles = fileListRepo.getUnloadedFiles();
        if (CollectionUtils.isEmpty(unloadFiles)) {
            return null;
        }
        FilePipePiece piece = new FilePipePiece();
        long totalBytes = 0;
        for (PipeFile file : ListUtils.emptyIfNull(unloadFiles)) {
            totalBytes += file.getSize();
            piece.addFile(file);
            if (totalBytes >= batchSize) {
                break;
            }
        }
        fileListRepo.updateFiles(piece.getFiles(), FileListRepo.PipeFileState.LOADING);

        return piece;
    }

    public void finishPiece(FilePipePiece piece, PipeTaskDesc.PipeTaskState taskState) {
        FileListRepo.PipeFileState state =
                taskState == PipeTaskDesc.PipeTaskState.ERROR ?
                        FileListRepo.PipeFileState.FAILED : FileListRepo.PipeFileState.LOADED;
        fileListRepo.updateFiles(piece.getFiles(), state);
    }

    public void setAutoIngest(boolean autoIngest) {
        this.autoIngest = autoIngest;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
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

    public FileListRepoInMemory getFileListRepo() {
        return fileListRepo;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.fileListRepo = new FileListRepoInMemory();
    }

    @Override
    public String toString() {
        return "FILE_SOURCE(path=" + path + ")";
    }
}
