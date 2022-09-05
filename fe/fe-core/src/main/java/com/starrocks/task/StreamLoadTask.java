// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/StreamLoadTask.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.task;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnSeparator;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ImportColumnDesc;
import com.starrocks.analysis.ImportColumnsStmt;
import com.starrocks.analysis.ImportWhereStmt;
import com.starrocks.analysis.RowDelimiter;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class StreamLoadTask {

    private static final Logger LOG = LogManager.getLogger(StreamLoadTask.class);

    private TUniqueId id;
    private long txnId;
    private TFileType fileType;
    private TFileFormatType formatType;
    private boolean stripOuterArray;
    private String jsonPaths;
    private String jsonRoot;

    // optional
    private List<ImportColumnDesc> columnExprDescs = Lists.newArrayList();
    private Expr whereExpr;
    private ColumnSeparator columnSeparator;
    private RowDelimiter rowDelimiter;
    private PartitionNames partitions;
    private String path;
    private boolean negative;
    private boolean strictMode = false; // default is false
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int timeout = Config.stream_load_default_timeout_second;
    private long execMemLimit = 0;
    private long loadMemLimit = 0;
    private boolean partialUpdate = false;
    private TCompressionType compressionType = TCompressionType.NO_COMPRESSION;
    private int loadParallelRequestNum = 0;
    private boolean enableReplicatedStorage = true;

    public StreamLoadTask(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
    }

    public TUniqueId getId() {
        return id;
    }

    public long getTxnId() {
        return txnId;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public TFileFormatType getFormatType() {
        return formatType;
    }

    public List<ImportColumnDesc> getColumnExprDescs() {
        return columnExprDescs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public String getPath() {
        return path;
    }

    public boolean getNegative() {
        return negative;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPath(String jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public void setJsonRoot(String jsonRoot) {
        this.jsonRoot = jsonRoot;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public void setPartialUpdate(boolean partialUpdate) {
        this.partialUpdate = partialUpdate;
    }

    public TCompressionType getTransmisionCompressionType() {
        return compressionType;
    }

    public boolean getEnableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public int getLoadParallelRequestNum() {
        return loadParallelRequestNum;
    }

    public static StreamLoadTask fromTStreamLoadPutRequest(TStreamLoadPutRequest request, Database db)
            throws UserException {
        StreamLoadTask streamLoadTask = new StreamLoadTask(request.getLoadId(), request.getTxnId(),
                request.getFileType(), request.getFormatType());
        streamLoadTask.setOptionalFromTSLPutRequest(request, db);
        return streamLoadTask;
    }

    private void setOptionalFromTSLPutRequest(TStreamLoadPutRequest request, Database db) throws UserException {
        if (request.isSetColumns()) {
            setColumnToColumnExpr(request.getColumns());
        }
        if (request.isSetWhere()) {
            setWhereExpr(request.getWhere());
        }
        if (request.isSetColumnSeparator()) {
            setColumnSeparator(request.getColumnSeparator());
        }
        if (request.isSetRowDelimiter()) {
            setRowDelimiter(request.getRowDelimiter());
        }
        if (request.isSetPartitions()) {
            String[] partNames = request.getPartitions().trim().split("\\s*,\\s*");
            if (request.isSetIsTempPartition()) {
                partitions = new PartitionNames(request.isIsTempPartition(), Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }
        switch (request.getFileType()) {
            case FILE_STREAM:
                path = request.getPath();
                break;
            default:
                throw new UserException("unsupported file type, type=" + request.getFileType());
        }
        if (request.isSetNegative()) {
            negative = request.isNegative();
        }
        if (request.isSetTimeout()) {
            timeout = request.getTimeout();
        }
        if (request.isSetStrictMode()) {
            strictMode = request.isStrictMode();
        }
        if (request.isSetTimezone()) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(request.getTimezone());
        }
        if (request.isSetLoadMemLimit()) {
            loadMemLimit = request.getLoadMemLimit();
        }
        if (request.getFormatType() == TFileFormatType.FORMAT_JSON) {
            if (request.getJsonpaths() != null) {
                jsonPaths = request.getJsonpaths();
            }
            if (request.getJson_root() != null) {
                jsonRoot = request.getJson_root();
            }
            stripOuterArray = request.isStrip_outer_array();
        }
        if (request.isSetPartial_update()) {
            partialUpdate = request.isPartial_update();
        }
        if (request.isSetTransmission_compression_type()) {
            compressionType = CompressionUtils.findTCompressionByName(request.getTransmission_compression_type());
        }
        if (request.isSetLoad_dop()) {
            loadParallelRequestNum = request.getLoad_dop();
        }
        if (request.isSetEnable_replicated_storage()) {
            enableReplicatedStorage = request.isEnable_replicated_storage();
        }
    }

    public static StreamLoadTask fromRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        TUniqueId dummyId = new TUniqueId();
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        if (routineLoadJob.getFormat().equals("json")) {
            fileFormatType = TFileFormatType.FORMAT_JSON;
        }
        StreamLoadTask streamLoadTask = new StreamLoadTask(dummyId, -1L /* dummy txn id*/,
                TFileType.FILE_STREAM, fileFormatType);
        streamLoadTask.setOptionalFromRoutineLoadJob(routineLoadJob);
        return streamLoadTask;
    }

    private void setOptionalFromRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        // copy the columnExprDescs, cause it may be changed when planning.
        // so we keep the columnExprDescs in routine load job as origin.
        if (routineLoadJob.getColumnDescs() != null) {
            columnExprDescs = Lists.newArrayList(routineLoadJob.getColumnDescs());
        }
        whereExpr = routineLoadJob.getWhereExpr();
        columnSeparator = routineLoadJob.getColumnSeparator();
        rowDelimiter = routineLoadJob.getRowDelimiter();
        partitions = routineLoadJob.getPartitions();
        strictMode = routineLoadJob.isStrictMode();
        timezone = routineLoadJob.getTimezone();
        timeout = (int) Config.routine_load_task_timeout_second;
        if (!routineLoadJob.getJsonPaths().isEmpty()) {
            jsonPaths = routineLoadJob.getJsonPaths();
        }
        if (!routineLoadJob.getJsonRoot().isEmpty()) {
            jsonRoot = routineLoadJob.getJsonRoot();
        }
        stripOuterArray = routineLoadJob.isStripOuterArray();
        partialUpdate = routineLoadJob.isPartialUpdate();
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.EXEC_MEM_LIMIT)) {
            execMemLimit = Long.parseLong(routineLoadJob.getSessionVariables().get(SessionVariable.EXEC_MEM_LIMIT));
        } else {
            execMemLimit = SessionVariable.DEFAULT_EXEC_MEM_LIMIT;
        }
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
            compressionType = CompressionUtils.findTCompressionByName(
                    routineLoadJob.getSessionVariables().get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
        }
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.ENABLE_REPLICATED_STORAGE)) {
            enableReplicatedStorage = Boolean
                    .parseBoolean(routineLoadJob.getSessionVariables().get(SessionVariable.ENABLE_REPLICATED_STORAGE));
        }
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws UserException {
        String columnsSQL = "COLUMNS (" + columns + ")";
        ImportColumnsStmt columnsStmt;
        try {
            columnsStmt = com.starrocks.sql.parser.SqlParser.parseImportColumns(columnsSQL, SqlModeHelper.MODE_DEFAULT);
        } catch (ParsingException e) {
            LOG.warn("parse columns' statement failed, sql={}, error={}", columnsSQL, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
            throw new UserException("parse columns header failed", e);
        }

        if (columnsStmt.getColumns() != null && !columnsStmt.getColumns().isEmpty()) {
            columnExprDescs = columnsStmt.getColumns();
        }
    }

    private void setWhereExpr(String whereString) throws UserException {
        ImportWhereStmt whereStmt;
        try {
            whereStmt = new ImportWhereStmt(com.starrocks.sql.parser.SqlParser.parseSqlToExpr(whereString,
                    SqlModeHelper.MODE_DEFAULT));
        } catch (ParsingException e) {
            LOG.warn("analyze where statement failed, sql={}, error={}", whereString, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse where header, sql={}", whereString, e);
            throw new UserException("parse columns header failed", e);
        }
        whereExpr = whereStmt.getExpr();
    }

    private void setColumnSeparator(String oriSeparator) throws AnalysisException {
        columnSeparator = new ColumnSeparator(oriSeparator);
        columnSeparator.analyze();
    }

    private void setRowDelimiter(String orgDelimiter) throws AnalysisException {
        rowDelimiter = new RowDelimiter(orgDelimiter);
        rowDelimiter.analyze();
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }
}
