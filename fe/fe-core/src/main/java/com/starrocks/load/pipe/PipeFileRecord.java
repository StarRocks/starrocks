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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.load.pipe.filelist.FileListRepo;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.logging.log4j.util.Strings;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Record stored in the pipe_files table
 */
public class PipeFileRecord {

    private static final String FILE_LOCATOR = "(pipe_id = %s AND file_name = %s AND file_version = %s)";
    private static final String FILE_RECORD_VALUES = "(%d, %s, %s, %d, %s, %s, %s, %s, %s)";

    public long pipeId;
    public String fileName;
    public String fileVersion;
    public long fileSize;

    public FileListRepo.PipeFileState loadState;
    public LocalDateTime lastModified;
    public LocalDateTime stagedTime;
    public LocalDateTime startLoadTime;
    public LocalDateTime finishLoadTime;

    public PipeFileRecord() {
    }

    public PipeFileRecord(long pipeId, String name, String version, long fileSize) {
        this.pipeId = pipeId;
        this.fileName = name;
        this.fileVersion = version;
        this.fileSize = fileSize;

        this.loadState = FileListRepo.PipeFileState.UNLOADED;
        this.lastModified = LocalDateTime.now();
        this.stagedTime = LocalDateTime.now();
        this.startLoadTime = LocalDateTime.now();
        this.finishLoadTime = LocalDateTime.now();
    }

    public static PipeFileRecord fromHdfsFile(FileStatus file) {
        PipeFileRecord record = new PipeFileRecord();
        record.fileName = file.getPath().toString();
        record.fileSize = file.getLen();
        if (file instanceof S3AFileStatus) {
            S3AFileStatus s3File = (S3AFileStatus) file;
            record.fileVersion = s3File.getEtag();
        } else {
            record.fileVersion = String.valueOf(file.getModificationTime());
        }
        record.lastModified = DateUtils.fromEpochMillis(file.getModificationTime());
        record.stagedTime = LocalDateTime.now();
        record.loadState = FileListRepo.PipeFileState.UNLOADED;
        return record;
    }

    public static List<PipeFileRecord> fromResultBatch(List<TResultBatch> batches) {
        List<PipeFileRecord> res = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                res.add(PipeFileRecord.fromJson(jsonString));
            }
        }
        return res;
    }

    /**
     * The json should come from the HTTP/JSON protocol, which looks like {"data": [col1, col2, col3]}
     */
    public static PipeFileRecord fromJson(String json) {
        try {
            JsonElement object = JsonParser.parseString(json);
            JsonArray dataArray = object.getAsJsonObject().get("data").getAsJsonArray();

            PipeFileRecord file = new PipeFileRecord();
            file.pipeId = dataArray.get(0).getAsLong();
            file.fileName = dataArray.get(1).getAsString();
            file.fileVersion = dataArray.get(2).getAsString();
            file.fileSize = dataArray.get(3).getAsLong();
            file.loadState =
                    EnumUtils.getEnumIgnoreCase(FileListRepo.PipeFileState.class, dataArray.get(4).getAsString());
            file.lastModified = parseJsonDateTime(dataArray.get(5));
            file.stagedTime = parseJsonDateTime(dataArray.get(6));
            file.startLoadTime = parseJsonDateTime(dataArray.get(7));
            file.finishLoadTime = parseJsonDateTime(dataArray.get(8));
            return file;
        } catch (Exception e) {
            throw new RuntimeException("convert json to PipeFile failed due to malformed json data: " + json, e);
        }
    }

    /**
     * Usually datetime in JSON should be string. but null datetime is a JSON null instead of empty string
     */
    public static LocalDateTime parseJsonDateTime(JsonElement json) throws AnalysisException {
        if (json.isJsonNull()) {
            return null;
        }
        String str = json.getAsString();
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        return DateUtils.parseDatTimeString(str);
    }

    public static String toSQLString(LocalDateTime dt) {
        if (dt == null) {
            return "NULL";
        }
        return Strings.quote(DateUtils.formatDateTimeUnix(dt));
    }

    public static String toSQLString(String str) {
        if (str == null) {
            return "NULL";
        } else {
            return Strings.quote(str);
        }
    }

    public static String toSQLStringNonnull(String str) {
        if (str == null) {
            return "''";
        } else {
            return Strings.quote(str);
        }
    }

    public String toValueList() {
        return String.format(FILE_RECORD_VALUES,
                pipeId,
                toSQLString(fileName),
                toSQLStringNonnull(fileVersion),
                fileSize,
                toSQLString(loadState.toString()),
                toSQLString(lastModified),
                toSQLString(stagedTime),
                toSQLString(startLoadTime),
                toSQLString(finishLoadTime)
        );
    }

    public String toUniqueLocator() {
        return String.format(FILE_LOCATOR,
                pipeId, Strings.quote(fileName), Strings.quote(fileVersion));
    }

    public long getPipeId() {
        return pipeId;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFileVersion() {
        return fileVersion;
    }

    public long getFileSize() {
        return fileSize;
    }

    public FileListRepo.PipeFileState getLoadState() {
        return loadState;
    }

    public LocalDateTime getLastModified() {
        return lastModified;
    }

    public LocalDateTime getStagedTime() {
        return stagedTime;
    }

    public LocalDateTime getStartLoadTime() {
        return startLoadTime;
    }

    public LocalDateTime getFinishLoadTime() {
        return finishLoadTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipeFileRecord that = (PipeFileRecord) o;
        return pipeId == that.pipeId && Objects.equals(fileName, that.fileName) &&
                Objects.equals(fileVersion, that.fileVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipeId, fileName, fileVersion);
    }

    @Override
    public String toString() {
        return "PipeFileRecord{" +
                "pipeId=" + pipeId +
                ", fileName='" + fileName + '\'' +
                ", fileVersion='" + fileVersion + '\'' +
                ", fileSize=" + fileSize +
                ", loadState=" + loadState +
                ", lastModified=" + lastModified +
                ", stagedTime=" + stagedTime +
                ", startLoadTime=" + startLoadTime +
                ", finishLoadTime=" + finishLoadTime +
                '}';
    }
}
